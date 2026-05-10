package com.example;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink 1.17 variant of the V1 schema-evolution demo. Same behavior as the
 * Flink 2.0 variant; the only differences are:
 * <ul>
 *   <li>{@code org.apache.flink.api.java.utils.ParameterTool} (Flink 1.17 location)</li>
 *   <li>{@code IdleAfterEmitSource} uses {@code RichParallelSourceFunction} from
 *       {@code org.apache.flink.streaming.api.functions.source} (no {@code .legacy.})
 *       and the pre-1.20 runtime-context API.</li>
 * </ul>
 */
public class Main {
    static final int MAX_BATCH_SIZE = 5000;
    static final int MAX_IN_FLIGHT_REQUESTS = 2;
    static final int MAX_BUFFERED_REQUESTS = 20000;
    static final long MAX_BATCH_SIZE_IN_BYTES = 1024 * 1024L;
    static final long MAX_TIME_IN_BUFFER_MS = 600_000L;  // 10 min — keep buffered until savepoint
    static final long MAX_RECORD_SIZE_IN_BYTES = 1000L;

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String url = params.get("url");
        String username = params.get("username");
        String password = params.get("password");
        String database = params.get("database");
        String table = params.get("table");
        int recordCount = params.getInt("records", 100);

        ClickHouseClientConfig clientConfig = new ClickHouseClientConfig(
                url, username, password, database, table);
        clientConfig.setSupportDefault(true);

        ClickHouseConvertor<EvolvingPOJO> converter = new ClickHouseConvertor<>(
                EvolvingPOJO.class, new EvolvingPOJOConvertor(true));

        ClickHouseAsyncSink<EvolvingPOJO> sink = ClickHouseAsyncSink.<EvolvingPOJO>builder()
                .setElementConverter(converter)
                .setMaxBatchSize(MAX_BATCH_SIZE)
                .setMaxInFlightRequests(MAX_IN_FLIGHT_REQUESTS)
                .setMaxBufferedRequests(MAX_BUFFERED_REQUESTS)
                .setMaxBatchSizeInBytes(MAX_BATCH_SIZE_IN_BYTES)
                .setMaxTimeInBufferMS(MAX_TIME_IN_BUFFER_MS)
                .setMaxRecordSizeInBytes(MAX_RECORD_SIZE_IN_BYTES)
                .setClickHouseClientConfig(clientConfig)
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2000);

        List<EvolvingPOJO> records = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            records.add(new EvolvingPOJO(i, "name-" + i));
        }

        env.addSource(new IdleAfterEmitSource<>(records))
                .returns(EvolvingPOJO.class)
                .uid("evolving-source").name("evolving-source")
                .sinkTo(sink).uid("clickhouse-sink").name("clickhouse-sink");

        env.execute("schema-evolution-v1");
    }
}
