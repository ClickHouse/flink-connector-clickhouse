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
 *   <li>{@link #setElementConverter} — a {@link ClickHouseConvertor}</li>
 *   <li>{@link #setClickHouseClientConfig}</li>
 * </ul>
 *
 * <p>Optional with defaults: batch tuning, format. In typed (POJO) mode the format
 * is forced to {@code RowBinaryWithNamesAndTypes} regardless of configuration.
 */
public class ClickHouseAsyncSinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<
                InputT, ClickHousePayload, ClickHouseAsyncSinkBuilder<InputT>> {

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
                elementConverter.isStringMode());
    }
}
