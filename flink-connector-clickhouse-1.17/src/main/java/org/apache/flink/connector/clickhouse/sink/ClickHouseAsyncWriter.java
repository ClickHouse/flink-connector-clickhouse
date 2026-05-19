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
import com.clickhouse.utils.writer.DataWriter;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.convertor.ColumnBinding;
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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class ClickHouseAsyncWriter<InputT>
        extends ExtendedAsyncSinkWriter<InputT, ClickHousePayload> {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseAsyncWriter.class);

    private final ClickHouseClientConfig clickHouseClientConfig;
    private final ClickHouseConvertor<InputT> convertor;
    private final ClickHouseFormat clickHouseFormat;          // for STRING mode only
    private RetryPolicy retryPolicy = RetryPolicy.forever();
    private final BatchFailureStrategy batchFailureStrategy = BatchFailureStrategy.STOP_FLINK;

    private final boolean typedMode;
    private final byte[] namesAndTypesHeader;                 // null in STRING mode

    private final Counter numBytesSendCounter;
    private final Counter numRecordsSendCounter;
    private final Counter numRequestSubmittedCounter;
    private final Counter numOfDroppedBatchesCounter;
    private final Counter numOfDroppedRecordsCounter;
    private final Counter totalBatchRetriesCounter;
    private final Histogram writeLatencyHistogram;
    private final Histogram writeFailureLatencyHistogram;

    public ClickHouseAsyncWriter(ElementConverter<InputT, ClickHousePayload> elementConverter,
                                 Sink.InitContext context,
                                 int maxBatchSize,
                                 int maxInFlightRequests,
                                 int maxBufferedRequests,
                                 long maxBatchSizeInBytes,
                                 long maxTimeInBufferMS,
                                 long maxRecordSizeInBytes,
                                 RetryPolicy retryPolicy,
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

        if (!(elementConverter instanceof ClickHouseConvertor)) {
            throw new IllegalArgumentException(
                "ClickHouseAsyncWriter requires a ClickHouseConvertor; got: "
                + elementConverter.getClass());
        }
        this.convertor = (ClickHouseConvertor<InputT>) elementConverter;
        this.clickHouseClientConfig = clickHouseClientConfig;
        this.clickHouseFormat = clickHouseFormat;
        this.retryPolicy = retryPolicy;
        this.typedMode = !this.convertor.isStringMode();

        if (typedMode) {
            if (clickHouseFormat != null && clickHouseFormat != ClickHouseFormat.RowBinaryWithNamesAndTypes) {
                LOG.warn("Typed sink ignores configured format {} — forcing RowBinaryWithNamesAndTypes",
                        clickHouseFormat);
            }
            this.namesAndTypesHeader = buildHeader(this.convertor.getBindings());
        } else {
            this.namesAndTypesHeader = null;
        }

        final SinkWriterMetricGroup metricGroup = context.metricGroup();
        this.numBytesSendCounter = metricGroup.getNumBytesSendCounter();
        this.numRecordsSendCounter = metricGroup.getNumRecordsSendCounter();
        this.numRequestSubmittedCounter = metricGroup.counter("numRequestSubmitted");
        this.numOfDroppedBatchesCounter = metricGroup.counter("numOfDroppedBatches");
        this.numOfDroppedRecordsCounter = metricGroup.counter("numOfDroppedRecords");
        this.totalBatchRetriesCounter = metricGroup.counter("totalBatchRetries");
        this.writeLatencyHistogram = metricGroup.histogram("writeLatencyHistogram",
                new DescriptiveStatisticsHistogram(1000));
        this.writeFailureLatencyHistogram = metricGroup.histogram("writeFailureLatencyHistogram",
                new DescriptiveStatisticsHistogram(1000));
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
        this(elementConverter, context, maxBatchSize, maxInFlightRequests, maxBufferedRequests,
             maxBatchSizeInBytes, maxTimeInBufferMS, maxRecordSizeInBytes,
             clickHouseClientConfig.getRetryPolicy(),
             clickHouseClientConfig, clickHouseFormat, state);
    }

    private static byte[] buildHeader(List<ColumnBinding> bindings) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            // RowBinaryWithNamesAndTypes wire format: varint column count,
            // then all column names (varint-prefixed) in order,
            // then all column types (varint-prefixed) in order.
            com.clickhouse.data.format.BinaryStreamUtils.writeVarInt(out, bindings.size());
            for (ColumnBinding b : bindings) {
                com.clickhouse.data.format.BinaryStreamUtils.writeString(out, b.columnName);
            }
            for (ColumnBinding b : bindings) {
                com.clickhouse.data.format.BinaryStreamUtils.writeString(out, b.column.getOriginalTypeName());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to build names+types header", e);
        }
        return baos.toByteArray();
    }

    private void rehydrateIfNeeded(List<ClickHousePayload> entries) throws IOException {
        ByteArrayOutputStream buf = null;
        DataWriter dw = null;
        for (ClickHousePayload p : entries) {
            if (!p.needsRehydration()) continue;
            if (p.isRaw()) {
                p.setCachedBytes((byte[]) p.getData().get(ClickHousePayload.RAW_KEY));
                continue;
            }
            // Typed mode: dispatch bindings against the restored Map.
            if (!typedMode) {
                throw new IOException(
                    "STRING-mode sink received a typed payload on restore — inconsistent state");
            }
            if (buf == null) { buf = new ByteArrayOutputStream(); dw = DataWriter.of(buf); }
            buf.reset();
            for (ColumnBinding b : convertor.getBindings()) {
                dw.writeValue(p.getData().get(b.mapKey), b.column);
            }
            p.setCachedBytes(buf.toByteArray());
        }
    }

    @Override
    protected void submitRequestEntries(List<ClickHousePayload> requestEntries,
                                        Consumer<List<ClickHousePayload>> requestToRetry) {
        this.numRequestSubmittedCounter.inc();
        LOG.info("Submitting {} request entries...", requestEntries.size());
        try {
            rehydrateIfNeeded(requestEntries);
        } catch (IOException e) {
            getFatalExceptionCons().accept(new FlinkWriteException("Rehydration failed", e));
            return;
        }

        Client chClient = this.clickHouseClientConfig.createClient();
        String tableName = clickHouseClientConfig.getTableName();

        final ClickHouseFormat format;
        if (typedMode) {
            format = ClickHouseFormat.RowBinaryWithNamesAndTypes;
        } else if (clickHouseFormat != null) {
            format = clickHouseFormat;
        } else {
            Boolean supportDefault = clickHouseClientConfig.getSupportDefault();
            if (supportDefault != null) {
                format = supportDefault ? ClickHouseFormat.RowBinaryWithDefaults : ClickHouseFormat.RowBinary;
            } else {
                throw new RuntimeException("ClickHouseFormat was not set");
            }
        }

        InsertSettings insertSettings = new InsertSettings();
        insertSettings.setOption(ClientConfigProperties.ASYNC_OPERATIONS.getKey(), "true");
        Boolean enableJsonAsString = clickHouseClientConfig.getEnableJsonSupportAsString();
        if (enableJsonAsString) {
            insertSettings.serverSetting(ServerSettings.INPUT_FORMAT_BINARY_READ_JSON_AS_STRING, "1");
        }
        if (typedMode) {
            // Server-side default substitution for Nullable + null per design §9c.
            insertSettings.serverSetting("input_format_null_as_default", "1");
            insertSettings.serverSetting("input_format_defaults_for_omitted_fields", "1");
        }

        long writeStartTime = System.currentTimeMillis();
        try {
            CompletableFuture<InsertResponse> response = chClient.insert(tableName, out -> {
                if (typedMode) {
                    out.write(namesAndTypesHeader);
                    this.numBytesSendCounter.inc(namesAndTypesHeader.length);
                }
                for (ClickHousePayload p : requestEntries) {
                    byte[] payload = p.getCachedBytes();
                    if (payload != null) {
                        this.numBytesSendCounter.inc(payload.length);
                        out.write(payload);
                    }
                }
                this.numRecordsSendCounter.inc(requestEntries.size());
                LOG.info("Data sent: bytes {}, records {}.",
                        numBytesSendCounter.getCount(), requestEntries.size());
                out.close();
            }, format, insertSettings);
            response.whenComplete((insertResponse, throwable) -> {
                if (throwable != null) {
                    handleFailedRequest(requestEntries, requestToRetry, throwable, writeStartTime);
                } else {
                    handleSuccessfulRequest(requestToRetry, insertResponse, writeStartTime);
                }
            });
        } catch (Exception e) {
            LOG.error("Error: ", e);
        }
        LOG.info("Finished submitting request entries.");
    }

    @Override
    protected long getSizeInBytes(ClickHousePayload p) {
        return p.getCachedBytesLength();
    }

    private void handleSuccessfulRequest(Consumer<List<ClickHousePayload>> requestToRetry,
                                         InsertResponse response, long writeStartTime) {
        long writeEndTime = System.currentTimeMillis();
        this.writeLatencyHistogram.update(writeEndTime - writeStartTime);
        LOG.info("Successfully completed submitting request. rows={}, serverTime={}, bytes={}, query_id={}, latency={}ms",
                response.getWrittenRows(), response.getServerTime(),
                response.getWrittenBytes(), response.getQueryId(),
                writeEndTime - writeStartTime);
        requestToRetry.accept(Collections.emptyList());
    }

    private void handleFailedRequest(List<ClickHousePayload> requestEntries,
                                     Consumer<List<ClickHousePayload>> requestToRetry,
                                     Throwable error, long writeStartTime) {
        LOG.error("Error while processing ClickHouse request", error);
        long writeEndTime = System.currentTimeMillis();
        this.writeFailureLatencyHistogram.update(writeEndTime - writeStartTime);
        try {
            Utils.handleException(error);
        } catch (RetriableException e) {
            LOG.info("Retriable exception occurred while processing request. ", e);
            if (requestEntries != null && !requestEntries.isEmpty()) {
                ClickHousePayload firstElement = requestEntries.get(0);
                firstElement.incrementAttempts();
                if (retryPolicy.isForever()) {
                    totalBatchRetriesCounter.inc();
                    LOG.warn("Retry forever number [{}]", firstElement.getAttemptCount());
                    requestToRetry.accept(requestEntries);
                } else {
                    if (firstElement.getAttemptCount() <= this.retryPolicy.getValue()) {
                        totalBatchRetriesCounter.inc();
                        LOG.warn("Retriable exception occurred. Left attempts {}.",
                                this.retryPolicy.getValue() - (firstElement.getAttemptCount() - 1));
                        requestToRetry.accept(requestEntries);
                    } else {
                        LOG.warn("Fatal — stop retrying, fail the Flink job", e);
                        getFatalExceptionCons().accept(e);
                    }
                }
            }
        } catch (DataCorruptionException e) {
            switch (this.batchFailureStrategy) {
                case DROP_BATCH:
                    LOG.info("Dropping {} entries due to non-retryable failure: {}",
                            requestEntries.size(), error.getLocalizedMessage());
                    numOfDroppedBatchesCounter.inc();
                    numOfDroppedRecordsCounter.inc(requestEntries.size());
                    requestToRetry.accept(Collections.emptyList());
                    break;
                case STOP_FLINK:
                    LOG.warn("Fatal — data corruption, fail the Flink job", e);
                    getFatalExceptionCons().accept(e);
                    break;
            }
        } catch (FlinkWriteException e) {
            LOG.warn("Fatal — stop retrying, fail the Flink job", e);
            getFatalExceptionCons().accept(e);
        }
    }
}
