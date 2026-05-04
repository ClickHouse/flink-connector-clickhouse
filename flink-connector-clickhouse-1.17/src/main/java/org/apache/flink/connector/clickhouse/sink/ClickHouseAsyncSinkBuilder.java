package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.data.ClickHouseFormat;

import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.util.Preconditions;

import java.util.Optional;

/**
 * Builder for {@link ClickHouseAsyncSink}.
 *
 * <p>Required:
 * <ul>
 *   <li>{@link #setElementConverter} — must be a {@link ClickHouseConvertor}; supplies
 *       both the conversion logic and the {@code TypeInformation} used for state
 *       serialization. Single source of truth for the input type.
 *   <li>{@link #setClickHouseClientConfig}
 * </ul>
 *
 * <p>Optional, with defaults:
 * <ul>
 *   <li>{@code maxBatchSize} = 500
 *   <li>{@code maxInFlightRequests} = 50
 *   <li>{@code maxBufferedRequests} = 10000
 *   <li>{@code maxBatchSizeInBytes} = 5 MB
 *   <li>{@code maxTimeInBufferMS} = 5000
 *   <li>{@code maxRecordSizeInBytes} = 1 MB
 *   <li>{@code clickHouseFormat} — falls back to {@code RowBinary} or
 *       {@code RowBinaryWithDefaults} based on {@code clickHouseClientConfig.getSupportDefault()}
 * </ul>
 */
public class ClickHouseAsyncSinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<
                InputT, ClickHousePayload<InputT>, ClickHouseAsyncSinkBuilder<InputT>> {

    private static final int DEFAULT_MAX_BATCH_SIZE = 500;
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 50;
    private static final int DEFAULT_MAX_BUFFERED_REQUESTS = 10_000;
    private static final long DEFAULT_MAX_BATCH_SIZE_IN_BYTES = 5L * 1024 * 1024;
    private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 5_000;
    private static final long DEFAULT_MAX_RECORD_SIZE_IN_BYTES = 1L * 1024 * 1024;

    private ClickHouseConvertor<InputT> elementConverter;
    private ClickHouseClientConfig clickHouseClientConfig;
    private ClickHouseFormat clickHouseFormat;

    ClickHouseAsyncSinkBuilder() {}

    public ClickHouseAsyncSinkBuilder<InputT> setElementConverter(
            ClickHouseConvertor<InputT> elementConverter) {
        this.elementConverter = elementConverter;
        return this;
    }

    public ClickHouseAsyncSinkBuilder<InputT> setClickHouseClientConfig(
            ClickHouseClientConfig clickHouseClientConfig) {
        this.clickHouseClientConfig = clickHouseClientConfig;
        return this;
    }

    public ClickHouseAsyncSinkBuilder<InputT> setClickHouseFormat(ClickHouseFormat clickHouseFormat) {
        this.clickHouseFormat = clickHouseFormat;
        return this;
    }

    @Override
    public ClickHouseAsyncSink<InputT> build() {
        Preconditions.checkNotNull(elementConverter, "elementConverter is required");
        Preconditions.checkNotNull(clickHouseClientConfig, "clickHouseClientConfig is required");

        return new ClickHouseAsyncSink<>(
                elementConverter,
                Optional.ofNullable(getMaxBatchSize()).orElse(DEFAULT_MAX_BATCH_SIZE),
                Optional.ofNullable(getMaxInFlightRequests()).orElse(DEFAULT_MAX_IN_FLIGHT_REQUESTS),
                Optional.ofNullable(getMaxBufferedRequests()).orElse(DEFAULT_MAX_BUFFERED_REQUESTS),
                Optional.ofNullable(getMaxBatchSizeInBytes()).orElse(DEFAULT_MAX_BATCH_SIZE_IN_BYTES),
                Optional.ofNullable(getMaxTimeInBufferMS()).orElse(DEFAULT_MAX_TIME_IN_BUFFER_MS),
                Optional.ofNullable(getMaxRecordSizeInBytes()).orElse(DEFAULT_MAX_RECORD_SIZE_IN_BYTES),
                clickHouseClientConfig,
                clickHouseFormat,
                elementConverter.getInputTypeInfo());
    }
}
