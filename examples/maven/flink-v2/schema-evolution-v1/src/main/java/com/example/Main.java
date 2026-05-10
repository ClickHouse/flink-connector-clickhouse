package com.example;

import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

import java.util.ArrayList;
import java.util.List;

/**
 * V1 release: writes EvolvingPOJO {id, name} into a 2-column ClickHouse
 * table. Long {@code maxTimeInBufferMS} keeps records buffered in writer
 * state so the upgrade workflow can take a savepoint and hand off to V2
 * without flushing first.
 *
 * <p>Args:
 * <ul>
 *   <li>{@code -url}        ClickHouse URL</li>
 *   <li>{@code -username}   ClickHouse user</li>
 *   <li>{@code -password}   ClickHouse password</li>
 *   <li>{@code -database}   ClickHouse database</li>
 *   <li>{@code -table}      ClickHouse table</li>
 *   <li>{@code -records}    optional, default 100</li>
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
