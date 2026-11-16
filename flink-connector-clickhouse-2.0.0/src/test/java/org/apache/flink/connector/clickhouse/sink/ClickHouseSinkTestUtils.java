package org.apache.flink.connector.clickhouse.sink;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ClickHouseSinkTestUtils {
    public static final int MAX_BATCH_SIZE = 5000;
    public static final int MIN_BATCH_SIZE = 1;
    public static final int MAX_IN_FLIGHT_REQUESTS = 2;
    public static final int MAX_BUFFERED_REQUESTS = 20000;
    public static final long MAX_BATCH_SIZE_IN_BYTES = 1024 * 1024;
    public static final long MAX_TIME_IN_BUFFER_MS = 5 * 1000;
    public static final long MAX_RECORD_SIZE_IN_BYTES = 1000;
    public static final int STREAM_PARALLELISM = 1;
    /** How many times, one second apart, {@link #runJob} counts the rows before it gives up. */
    public static final int RUN_JOB_POLLS = 10;

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

    /**
     * Run a small Flink mini-cluster job that ships a list through the sink, and wait until the
     * table holds {@code expectedRows} rows.
     *
     * <p>The job's own failure never reaches the table, so a bare row count cannot tell a rejected
     * write from a slow one. When the count does not match, the assertion therefore names the job
     * status, and a failed job's error (for a rejected write, the ClickHouse {@code ServerException}
     * with its code) becomes the cause. A failed job ends the wait at once.
     */
    public static <T> void runJob(ClickHouseAsyncSink<T> sink, List<T> rows, String tableName,
                                  int expectedRows) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(STREAM_PARALLELISM);
        DataStream<T> stream = env.fromCollection(rows);
        stream.sinkTo(sink);
        JobClient jobClient = env.executeAsync("runJob " + tableName);
        try {
            int inserted = -1;
            JobStatus status = null;
            for (int poll = 0; poll < RUN_JOB_POLLS; poll++) {
                Thread.sleep(1000);
                inserted = ClickHouseServerForTests.countRows(tableName);
                if (inserted == expectedRows) {
                    return;
                }
                status = jobClient.getJobStatus().get(10, TimeUnit.SECONDS);
                if (status == JobStatus.FAILED) {
                    break;
                }
            }
            Assertions.fail(String.format("Table %s holds %d rows, expected %d; the Flink job is %s",
                    tableName, inserted, expectedRows, status), jobFailure(jobClient, status));
        } finally {
            jobClient.cancel();
        }
    }

    /** The error a FAILED job ended with; null for any other status. */
    private static Throwable jobFailure(JobClient jobClient, JobStatus status) throws Exception {
        if (status != JobStatus.FAILED) {
            return null;
        }
        try {
            jobClient.getJobExecutionResult().get(10, TimeUnit.SECONDS);
            return null;
        } catch (ExecutionException e) {
            return e.getCause();
        }
    }
}
