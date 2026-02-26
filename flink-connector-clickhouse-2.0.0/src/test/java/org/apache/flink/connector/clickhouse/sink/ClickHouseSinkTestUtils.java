package org.apache.flink.connector.clickhouse.sink;

public class ClickHouseSinkTestUtils {
    public static final int MAX_BATCH_SIZE = 5000;
    public static final int MIN_BATCH_SIZE = 1;
    public static final int MAX_IN_FLIGHT_REQUESTS = 2;
    public static final int MAX_BUFFERED_REQUESTS = 20000;
    public static final long MAX_BATCH_SIZE_IN_BYTES = 1024 * 1024;
    public static final long MAX_TIME_IN_BUFFER_MS = 5 * 1000;
    public static final long MAX_RECORD_SIZE_IN_BYTES = 1000;
}
