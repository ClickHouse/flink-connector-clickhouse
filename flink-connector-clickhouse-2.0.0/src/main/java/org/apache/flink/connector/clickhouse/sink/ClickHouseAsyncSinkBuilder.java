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
 *
 * <p>{@link #build()} pings ClickHouse and fails fast if it is unreachable from the job
 * driver; {@link #setVerifyConnectivity} turns that check off.
 */
public class ClickHouseAsyncSinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<
                InputT, ClickHousePayload, ClickHouseAsyncSinkBuilder<InputT>> {

    private ClickHouseConvertor<InputT> elementConverter;
    private ClickHouseClientConfig clickHouseClientConfig;
    private ClickHouseFormat clickHouseFormat;
    private boolean verifyConnectivity = true;

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

    /**
     * Whether {@link #build()} pings ClickHouse and fails fast when it is unreachable
     * (default {@code true}). Turn off when the job driver cannot reach the server or
     * connectivity was already verified, as the Table API factory does at planning.
     */
    public ClickHouseAsyncSinkBuilder<InputT> setVerifyConnectivity(boolean verifyConnectivity) {
        this.verifyConnectivity = verifyConnectivity;
        return this;
    }

    @Override
    public ClickHouseAsyncSink<InputT> build() {
        Preconditions.checkNotNull(elementConverter, "elementConverter is required");
        Preconditions.checkNotNull(clickHouseClientConfig, "clickHouseClientConfig is required");
        if (verifyConnectivity) {
            clickHouseClientConfig.verifyConnectivity();
        }

        return new ClickHouseAsyncSink<>(
                elementConverter,
                Optional.ofNullable(getMaxBatchSize()).orElse(ClickHouseSinkDefaults.MAX_BATCH_SIZE),
                Optional.ofNullable(getMaxInFlightRequests()).orElse(ClickHouseSinkDefaults.MAX_IN_FLIGHT_REQUESTS),
                Optional.ofNullable(getMaxBufferedRequests()).orElse(ClickHouseSinkDefaults.MAX_BUFFERED_REQUESTS),
                Optional.ofNullable(getMaxBatchSizeInBytes()).orElse(ClickHouseSinkDefaults.MAX_BATCH_SIZE_IN_BYTES),
                Optional.ofNullable(getMaxTimeInBufferMS()).orElse(ClickHouseSinkDefaults.MAX_TIME_IN_BUFFER_MS),
                Optional.ofNullable(getMaxRecordSizeInBytes()).orElse(ClickHouseSinkDefaults.MAX_RECORD_SIZE_IN_BYTES),
                clickHouseClientConfig,
                clickHouseFormat,
                elementConverter.isStringMode());
    }
}
