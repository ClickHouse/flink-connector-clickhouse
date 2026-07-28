package org.apache.flink.connector.clickhouse.sink;

import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Assertions;

import java.util.List;

import static org.apache.flink.connector.test.embedded.flink.EmbeddedFlinkClusterForTests.executeAsyncJob;

public class ClickHouseSinkTestUtils {
    public static final int MAX_BATCH_SIZE = 5000;
    public static final int MIN_BATCH_SIZE = 1;
    public static final int MAX_IN_FLIGHT_REQUESTS = 2;
    public static final int MAX_BUFFERED_REQUESTS = 20000;
    public static final long MAX_BATCH_SIZE_IN_BYTES = 1024 * 1024;
    public static final long MAX_TIME_IN_BUFFER_MS = 5 * 1000;
    public static final long MAX_RECORD_SIZE_IN_BYTES = 1000;
    public static final int STREAM_PARALLELISM = 1;

    /** Construct a sink with the standard test-batch tuning + a typed converter. */
    public static <T> ClickHouseAsyncSink<T> buildSink(ClickHouseConvertor<T> convertor, String tableName) {
        ClickHouseClientConfig clientConfig = new ClickHouseClientConfig(
            ClickHouseServerForTests.getURL(),
            ClickHouseServerForTests.getUsername(),
            ClickHouseServerForTests.getPassword(),
            ClickHouseServerForTests.getDatabase(),
            tableName);
        return ClickHouseAsyncSink.<T>builder()
            .setElementConverter(convertor)
            .setMaxBatchSize(MAX_BATCH_SIZE)
            .setMaxInFlightRequests(MAX_IN_FLIGHT_REQUESTS)
            .setMaxBufferedRequests(MAX_BUFFERED_REQUESTS)
            .setMaxBatchSizeInBytes(MAX_BATCH_SIZE_IN_BYTES)
            .setMaxTimeInBufferMS(MAX_TIME_IN_BUFFER_MS)
            .setMaxRecordSizeInBytes(MAX_RECORD_SIZE_IN_BYTES)
            .setClickHouseClientConfig(clientConfig)
            .build();
    }

    /** Run a small Flink mini-cluster job that ships a list through the sink. */
    public static <T> void runJob(ClickHouseAsyncSink<T> sink, List<T> rows, String tableName,
                                  int expectedRows) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(STREAM_PARALLELISM);
        DataStream<T> stream = env.fromCollection(rows);
        stream.sinkTo(sink);
        int inserted = executeAsyncJob(env, tableName, 10, expectedRows);
        Assertions.assertEquals(expectedRows, inserted);
    }
}
