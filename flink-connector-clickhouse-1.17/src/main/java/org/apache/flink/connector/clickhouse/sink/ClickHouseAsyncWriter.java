package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.utils.Utils;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.connector.clickhouse.exception.RetriableException;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.Objects;
import java.util.function.Consumer;


public class ClickHouseAsyncWriter<InputT> extends AsyncSinkWriter<InputT, ClickHousePayload> {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseAsyncWriter.class);
    private static final int DEFAULT_MAX_RETRIES = 3;

    private final ClickHouseClientConfig clickHouseClientConfig;
    private ClickHouseFormat clickHouseFormat = null;
    private int numberOfRetries = DEFAULT_MAX_RETRIES;

    private final Counter numBytesSendCounter;
    private final Counter numRecordsSendCounter;
    private final Counter numRequestSubmittedCounter;
    private final Counter numOfDroppedBatchesCounter;
    private final Counter numOfDroppedRecordsCounter;
    private final Counter totalBatchRetriesCounter;

    public ClickHouseAsyncWriter(ElementConverter<InputT, ClickHousePayload> elementConverter,
                                 Sink.InitContext context,
                                 int maxBatchSize,
                                 int maxInFlightRequests,
                                 int maxBufferedRequests,
                                 long maxBatchSizeInBytes,
                                 long maxTimeInBufferMS,
                                 long maxRecordSizeInBytes,
                                 int numberOfRetries,
                                 ClickHouseClientConfig clickHouseClientConfig,
                                 ClickHouseFormat clickHouseFormat,
                                 Collection<BufferedRequestState<ClickHousePayload>> state) {
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
        this.clickHouseFormat = clickHouseFormat;
        this.numberOfRetries = numberOfRetries;
        final SinkWriterMetricGroup metricGroup = context.metricGroup();
        this.numBytesSendCounter = metricGroup.getNumBytesSendCounter();
        this.numRecordsSendCounter = metricGroup.getNumRecordsSendCounter();
        this.numRequestSubmittedCounter = metricGroup.counter("numRequestSubmitted");
        this.numOfDroppedBatchesCounter = metricGroup.counter("numOfDroppedBatches");
        this.numOfDroppedRecordsCounter = metricGroup.counter("numOfDroppedRecords");
        this.totalBatchRetriesCounter = metricGroup.counter("totalBatchRetries");
    }


    public ClickHouseAsyncWriter(ElementConverter<InputT, ClickHousePayload> elementConverter,
                                 Sink.InitContext context,
                                 int maxBatchSize,
                                 int maxInFlightRequests,
                                 int maxBufferedRequests,
                                 long maxBatchSizeInBytes,
                                 long maxTimeInBufferMS,
                                 long maxRecordSizeInBytes,
                                 ClickHouseClientConfig clickHouseClientConfig,
                                 ClickHouseFormat clickHouseFormat,
                                 Collection<BufferedRequestState<ClickHousePayload>> state) {
        this(elementConverter,
             context,
             maxBatchSize,
             maxInFlightRequests,
             maxBufferedRequests,
             maxBatchSizeInBytes,
             maxTimeInBufferMS,
             maxRecordSizeInBytes,
             clickHouseClientConfig.getNumberOfRetries(),
             clickHouseClientConfig,
             clickHouseFormat,
             state
        );
    }

    @Override
    protected void submitRequestEntries(List<ClickHousePayload> requestEntries, Consumer<List<ClickHousePayload>> requestToRetry) {
        this.numRequestSubmittedCounter.inc();
        LOG.info("Submitting request entries...");
        Client chClient = this.clickHouseClientConfig.createClient();
        String tableName = clickHouseClientConfig.getTableName();
        ClickHouseFormat format = null;
        if (clickHouseFormat != null) {
            format = clickHouseFormat;
        } else {
            // TODO: check if we configured payload to POJO serialization.
            // this not define lets try to get it from ClickHousePayload in case  of POJO can be RowBinary or RowBinaryWithDefaults
            Boolean supportDefault = clickHouseClientConfig.getSupportDefault();
            if (supportDefault != null) {
                if (supportDefault) format = ClickHouseFormat.RowBinaryWithDefaults;
                else format = ClickHouseFormat.RowBinary;
            } else {
                throw new RuntimeException("ClickHouseFormat was not set ");
            }
        }
        try {
            CompletableFuture<InsertResponse> response = chClient.insert(tableName, out -> {
                for (ClickHousePayload requestEntry : requestEntries) {
                    if (requestEntry.getPayload() != null) {
                        byte[] payload = requestEntry.getPayload();
                        // sum the data that is sent to ClickHouse
                        this.numBytesSendCounter.inc(payload.length);
                        out.write(payload);
                    }
                }
                // send the number that is sent to ClickHouse
                this.numRecordsSendCounter.inc(requestEntries.size());
                LOG.info("Data that will be sent to ClickHouse in bytes {} and the amount of records {}.", numBytesSendCounter.getCount(), requestEntries.size());
                out.close();
            }, format, new InsertSettings().setOption(ClientConfigProperties.ASYNC_OPERATIONS.getKey(), "true"));
            response.whenComplete((insertResponse, throwable) -> {
                if (throwable != null) {
                    handleFailedRequest(requestEntries, requestToRetry, throwable);
                } else {
                    handleSuccessfulRequest(requestToRetry, insertResponse);
                }
            });
        } catch (Exception e) {
            LOG.error("Error: ", e);
        }
        LOG.info("Finished submitting request entries.");
    }

    @Override
    protected long getSizeInBytes(ClickHousePayload clickHousePayload) {
        return clickHousePayload.getPayloadLength();
    }


    private void handleSuccessfulRequest(
            Consumer<List<ClickHousePayload>> requestToRetry, InsertResponse response) {
        LOG.info("Successfully completed submitting request. Response [written rows {}, time took to insert {}, written bytes {}, query_id {}]",
                response.getWrittenRows(),
                response.getServerTime(),
                response.getWrittenBytes(),
                response.getQueryId()
        );
        requestToRetry.accept(Collections.emptyList());
    }

    private void handleFailedRequest(
            List<ClickHousePayload> requestEntries,
            Consumer<List<ClickHousePayload>> requestToRetry,
            Throwable error) {
        // TODO: extract from error if we can retry
        try {
            Utils.handleException(error);
        } catch (RetriableException e) {
            LOG.info("Retriable exception occurred while processing request. ", e);
            // Let's try to retry
            if (requestEntries != null && !requestEntries.isEmpty()) {
                ClickHousePayload firstElement = requestEntries.get(0);
                LOG.warn("Retry number [{}] out of [{}]", firstElement.getAttemptCount(), this.numberOfRetries);
                firstElement.incrementAttempts();
                if (firstElement.getAttemptCount() <= this.numberOfRetries) {
                    totalBatchRetriesCounter.inc();
                    LOG.warn("Retriable exception occurred while processing request. Left attempts {}.", this.numberOfRetries - (firstElement.getAttemptCount() - 1) );
                    // We are not in retry threshold we can send data again
                    requestToRetry.accept(requestEntries);
                    return;
                } else {
                    LOG.warn("No attempts left going to drop batch");
                }
            }

        }
        LOG.info("Dropping {} request entries due to non-retryable failure: {}", requestEntries.size(), error.getLocalizedMessage());
        numOfDroppedBatchesCounter.inc();
        numOfDroppedRecordsCounter.inc(requestEntries.size());
        // since we do not want retry again we send empty list to the queue
        requestToRetry.accept(Collections.emptyList());
    }

}
