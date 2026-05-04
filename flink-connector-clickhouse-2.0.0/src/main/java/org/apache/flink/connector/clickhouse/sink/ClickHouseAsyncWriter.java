package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.config.BatchFailureStrategy;
import com.clickhouse.config.RetryPolicy;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.utils.Utils;

import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.connector.clickhouse.exception.DataCorruptionException;
import org.apache.flink.connector.clickhouse.exception.FlinkWriteException;
import org.apache.flink.connector.clickhouse.exception.RetriableException;
import org.apache.flink.connector.clickhouse.sink.writer.ExtendedAsyncSinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;


public class ClickHouseAsyncWriter<InputT>
        extends ExtendedAsyncSinkWriter<InputT, ClickHousePayload<InputT>> {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseAsyncWriter.class);
    private static final int DEFAULT_MAX_RETRIES = 3;

    private final ClickHouseClientConfig clickHouseClientConfig;
    private final ElementConverter<InputT, ClickHousePayload<InputT>> elementConverter;
    private ClickHouseFormat clickHouseFormat = null;
    private RetryPolicy retryPolicy = RetryPolicy.forever();
    private BatchFailureStrategy batchFailureStrategy = BatchFailureStrategy.STOP_FLINK;

    private final Counter numBytesSendCounter;
    private final Counter numRecordsSendCounter;
    private final Counter numRequestSubmittedCounter;
    private final Counter numOfDroppedBatchesCounter;
    private final Counter numOfDroppedRecordsCounter;
    private final Counter totalBatchRetriesCounter;
    private final Histogram writeLatencyHistogram;
    private final Histogram writeFailureLatencyHistogram;

    public ClickHouseAsyncWriter(ElementConverter<InputT, ClickHousePayload<InputT>> elementConverter,
                                 WriterInitContext context,
                                 int maxBatchSize,
                                 int maxInFlightRequests,
                                 int maxBufferedRequests,
                                 long maxBatchSizeInBytes,
                                 long maxTimeInBufferMS,
                                 long maxRecordSizeInBytes,
                                 RetryPolicy retryPolicy,
                                 ClickHouseClientConfig clickHouseClientConfig,
                                 ClickHouseFormat clickHouseFormat,
                                 Collection<BufferedRequestState<ClickHousePayload<InputT>>> state) {
        super(elementConverter,
                context,
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(maxBatchSize)
                        .setMaxBatchSizeInBytes(maxBatchSizeInBytes)
                        .setMaxInFlightRequests(maxInFlightRequests)
                        .setMaxBufferedRequests(maxBufferedRequests)
                        .setMaxTimeInBufferMS(maxTimeInBufferMS)
                        .setMaxRecordSizeInBytes(maxRecordSizeInBytes)
                        .build(),
                state);
        this.clickHouseClientConfig = clickHouseClientConfig;
        this.elementConverter = elementConverter;
        this.clickHouseFormat = clickHouseFormat;
        this.retryPolicy = retryPolicy;
        final SinkWriterMetricGroup metricGroup = context.metricGroup();
        this.numBytesSendCounter = metricGroup.getNumBytesSendCounter();
        this.numRecordsSendCounter = metricGroup.getNumRecordsSendCounter();
        this.numRequestSubmittedCounter = metricGroup.counter("numRequestSubmitted");
        this.numOfDroppedBatchesCounter = metricGroup.counter("numOfDroppedBatches");
        this.numOfDroppedRecordsCounter = metricGroup.counter("numOfDroppedRecords");
        this.totalBatchRetriesCounter = metricGroup.counter("totalBatchRetries");
        this.writeLatencyHistogram = metricGroup.histogram("writeLatencyHistogram", new DescriptiveStatisticsHistogram(1000));
        this.writeFailureLatencyHistogram = metricGroup.histogram("writeFailureLatencyHistogram", new DescriptiveStatisticsHistogram(1000));
    }


    public ClickHouseAsyncWriter(ElementConverter<InputT, ClickHousePayload<InputT>> elementConverter,
                                 WriterInitContext context,
                                 int maxBatchSize,
                                 int maxInFlightRequests,
                                 int maxBufferedRequests,
                                 long maxBatchSizeInBytes,
                                 long maxTimeInBufferMS,
                                 long maxRecordSizeInBytes,
                                 ClickHouseClientConfig clickHouseClientConfig,
                                 ClickHouseFormat clickHouseFormat,
                                 Collection<BufferedRequestState<ClickHousePayload<InputT>>> state) {
        this(elementConverter,
             context,
             maxBatchSize,
             maxInFlightRequests,
             maxBufferedRequests,
             maxBatchSizeInBytes,
             maxTimeInBufferMS,
             maxRecordSizeInBytes,
             clickHouseClientConfig.getRetryPolicy(),
             clickHouseClientConfig,
             clickHouseFormat,
             state
        );
    }

    /**
     * Lazy rehydration helper: V2-restored entries arrive with {@code data} only and no
     * {@code cachedBytes}. This loop produces bytes via the current converter (picking up
     * any deployed bug fix or schema change since the checkpoint was written) before send.
     *
     * <p>Package-private + static so it can be unit-tested without constructing a writer.
     *
     * <p>Mutates entries in place: sets {@code cachedBytes} on those that need it, leaves
     * V1-restored and steady-state entries untouched.
     */
    static <T> void rehydrateIfNeeded(
            List<ClickHousePayload<T>> entries,
            ElementConverter<T, ClickHousePayload<T>> converter) {
        for (ClickHousePayload<T> entry : entries) {
            if (entry.needsRehydration()) {
                ClickHousePayload<T> fresh = converter.apply(entry.getData(), null);
                entry.setCachedBytes(fresh.getCachedBytes());
            }
        }
    }

    @Override
    protected long getSizeInBytes(ClickHousePayload<InputT> clickHousePayload) {
        return clickHousePayload.getCachedBytesLength();
    }

    @Override
    protected void submitRequestEntries(List<ClickHousePayload<InputT>> requestEntries,
                                        ResultHandler<ClickHousePayload<InputT>> resultHandler) {
        this.numRequestSubmittedCounter.inc();
        LOG.info("Submitting request entries...");

        rehydrateIfNeeded(requestEntries, elementConverter);

        Client chClient = this.clickHouseClientConfig.createClient();
        String tableName = clickHouseClientConfig.getTableName();
        ClickHouseFormat format;
        if (clickHouseFormat != null) {
            format = clickHouseFormat;
        } else {
            Boolean supportDefault = clickHouseClientConfig.getSupportDefault();
            if (supportDefault != null) {
                format = supportDefault ? ClickHouseFormat.RowBinaryWithDefaults : ClickHouseFormat.RowBinary;
            } else {
                throw new RuntimeException("ClickHouseFormat was not set ");
            }
        }
        InsertSettings insertSettings = new InsertSettings();
        insertSettings.setOption(ClientConfigProperties.ASYNC_OPERATIONS.getKey(), "true");
        Boolean getEnableJsonSupportAsString = clickHouseClientConfig.getEnableJsonSupportAsString();
        if (getEnableJsonSupportAsString) {
            insertSettings.serverSetting(ServerSettings.INPUT_FORMAT_BINARY_READ_JSON_AS_STRING, "1");
        }
        long writeStartTime = System.currentTimeMillis();
        try {
            CompletableFuture<InsertResponse> response = chClient.insert(tableName, out -> {
                for (ClickHousePayload<InputT> requestEntry : requestEntries) {
                    if (requestEntry.getCachedBytes() != null) {
                        byte[] payload = requestEntry.getCachedBytes();
                        this.numBytesSendCounter.inc(payload.length);
                        out.write(payload);
                    }
                }
                this.numRecordsSendCounter.inc(requestEntries.size());
                LOG.info("Data that will be sent to ClickHouse in bytes {} and the amount of records {}.",
                        numBytesSendCounter.getCount(), requestEntries.size());
                out.close();
            }, format, insertSettings);
            response.whenComplete((insertResponse, throwable) -> {
                if (throwable != null) {
                    handleFailedRequest(requestEntries, resultHandler, throwable, writeStartTime);
                } else {
                    handleSuccessfulRequest(resultHandler, insertResponse, writeStartTime);
                }
            });
        } catch (Exception e) {
            LOG.error("Error: ", e);
        }
        LOG.info("Finished submitting request entries.");
    }

    private void handleSuccessfulRequest(
            ResultHandler<ClickHousePayload<InputT>> resultHandler, InsertResponse response, long writeStartTime) {
        long writeEndTime = System.currentTimeMillis();
        this.writeLatencyHistogram.update(writeEndTime - writeStartTime);
        LOG.info("Successfully completed submitting request. Response [written rows {}, time took to insert {}, written bytes {}, query_id {}, write latency {} ms.]",
                response.getWrittenRows(),
                response.getServerTime(),
                response.getWrittenBytes(),
                response.getQueryId(),
                writeEndTime - writeStartTime
        );
        resultHandler.complete();
    }

    private void handleFailedRequest(
            List<ClickHousePayload<InputT>> requestEntries,
            ResultHandler<ClickHousePayload<InputT>> resultHandler,
            Throwable error, long writeStartTime) {
        LOG.error("Error while processing ClickHouse request", error);
        long writeEndTime = System.currentTimeMillis();
        this.writeFailureLatencyHistogram.update(writeEndTime - writeStartTime);
        try {
            Utils.handleException(error);
        } catch (RetriableException e) {
            LOG.info("Retriable exception occurred while processing request. ", e);
            if (requestEntries != null && !requestEntries.isEmpty()) {
                ClickHousePayload<InputT> firstElement = requestEntries.get(0);
                firstElement.incrementAttempts();
                if (retryPolicy.isForever()) {
                    totalBatchRetriesCounter.inc();
                    LOG.warn("Retry forever number [{}]", firstElement.getAttemptCount());
                    resultHandler.retryForEntries(requestEntries);
                } else {
                    if (firstElement.getAttemptCount() <= this.retryPolicy.getValue()) {
                        totalBatchRetriesCounter.inc();
                        LOG.warn("Retriable exception occurred while processing request. Left attempts {}.",
                                this.retryPolicy.getValue() - (firstElement.getAttemptCount() - 1));
                        resultHandler.retryForEntries(requestEntries);
                    } else {
                        LOG.warn("Fatal — stop retrying, fail the Flink job", e);
                        resultHandler.completeExceptionally((Exception) e);
                    }
                }
            }
        } catch (DataCorruptionException e) {
            switch (this.batchFailureStrategy) {
                case DROP_BATCH:
                    LOG.info("Dropping {} request entries due to non-retryable failure: {}",
                            requestEntries.size(), error.getLocalizedMessage());
                    numOfDroppedBatchesCounter.inc();
                    numOfDroppedRecordsCounter.inc(requestEntries.size());
                    resultHandler.complete();
                    break;
                case STOP_FLINK:
                    LOG.warn("Fatal — data corruption, fail the Flink job", e);
                    resultHandler.completeExceptionally((Exception) e);
                    break;
                // TODO: DLQ implementation
            }
        } catch (FlinkWriteException e) {
            LOG.warn("Fatal — stop retrying, fail the Flink job", e);
            resultHandler.completeExceptionally((Exception) e);
        }
    }
}
