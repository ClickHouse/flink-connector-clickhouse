package org.apache.flink.connector.clickhouse.sink;

/** Batching/backpressure defaults shared by ClickHouseAsyncSinkBuilder and the SQL connector options. */
public final class ClickHouseSinkDefaults {

    public static final int MAX_BATCH_SIZE = 500;
    public static final int MAX_IN_FLIGHT_REQUESTS = 50;
    public static final int MAX_BUFFERED_REQUESTS = 10_000;
    public static final long MAX_BATCH_SIZE_IN_BYTES = 5L * 1024 * 1024;
    public static final long MAX_TIME_IN_BUFFER_MS = 5_000;
    public static final long MAX_RECORD_SIZE_IN_BYTES = 1L * 1024 * 1024;

    private ClickHouseSinkDefaults() {}
}
