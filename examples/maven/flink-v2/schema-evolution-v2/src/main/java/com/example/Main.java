package com.example;

import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

import java.util.ArrayList;
import java.util.List;

/**
 * V2 release: writes EvolvingPOJO {id, name, ts} to a 3-column ClickHouse
 * table. Started from a savepoint produced by the V1 release; on restore,
 * Flink's {@code PojoSerializerSnapshot} migrates buffered V1 state into
 * V2 instances with {@code ts=0}, then this job's convertor encodes them
 * for the post-ALTER table.
 *
 * <p>Short {@code maxTimeInBufferMS} so restored records flush to ClickHouse
 * promptly. The source emits a fresh batch of native-V2 records (with
 * {@code ts<>0}) on top of the restored ones to verify the upgrade flow
 * supports both cohorts.
 *
 * <p>Args:
 * <ul>
 *   <li>{@code -url}        ClickHouse URL</li>
 *   <li>{@code -username}   ClickHouse user</li>
 *   <li>{@code -password}   ClickHouse password</li>
 *   <li>{@code -database}   ClickHouse database</li>
 *   <li>{@code -table}      ClickHouse table</li>
 *   <li>{@code -records}    optional, default 100 (number of fresh records emitted)</li>
 *   <li>{@code -idStart}    optional, default 100 (id of the first fresh V2 record)</li>
 *   <li>{@code -ts}         optional, default 42000 (ts value for fresh V2 records)</li>
 * </ul>
 */
public class Main {
    static final int MAX_BATCH_SIZE = 5000;
    static final int MAX_IN_FLIGHT_REQUESTS = 2;
    static final int MAX_BUFFERED_REQUESTS = 20000;
    static final long MAX_BATCH_SIZE_IN_BYTES = 1024 * 1024L;
    static final long MAX_TIME_IN_BUFFER_MS = 5_000L;  // short — flush after restore
    static final long MAX_RECORD_SIZE_IN_BYTES = 1000L;

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String url = params.get("url");
        String username = params.get("username");
        String password = params.get("password");
        String database = params.get("database");
        String table = params.get("table");
        int recordCount = params.getInt("records", 100);
        int idStart = params.getInt("idStart", 100);
        long ts = params.getLong("ts", 42000L);

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
            int id = idStart + i;
            records.add(new EvolvingPOJO(id, "name-" + id, ts));
        }

        env.addSource(new IdleAfterEmitSource<>(records))
                .returns(EvolvingPOJO.class)
                .uid("evolving-source").name("evolving-source")
                .sinkTo(sink).uid("clickhouse-sink").name("clickhouse-sink");

        env.execute("schema-evolution-v2");
    }
}
