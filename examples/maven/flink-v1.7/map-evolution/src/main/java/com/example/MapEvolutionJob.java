package com.example;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.convertor.DataMapper;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Demonstrates schema evolution via the user's {@link DataMapper}. See
 * {@code README.md} for the run sequence.
 *
 * <p>The {@code -build} arg selects mapper A or B; the rest of the job is identical.
 * That's the point: a Build-A checkpoint restored under Build B is read as a Map
 * with no {@code c} key, and Build B's mapper writes the new column safely.
 */
public class MapEvolutionJob {

    static final int MAX_BATCH_SIZE = 10_000;
    static final int MAX_IN_FLIGHT_REQUESTS = 2;
    static final int MAX_BUFFERED_REQUESTS = 50_000;
    static final long MAX_BATCH_SIZE_IN_BYTES = 5L * 1024 * 1024;
    static final long MAX_RECORD_SIZE_IN_BYTES = 1024L;

    // Build A: hold records for 10 minutes so the user can take a savepoint
    //          before any data reaches ClickHouse.
    // Build B: flush quickly (5 seconds) after restore so the migrated state +
    //          fresh emissions land in ClickHouse without waiting.
    static final long BUILD_A_BUFFER_MS = 600_000L;
    static final long BUILD_B_BUFFER_MS = 5_000L;

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        final String url      = parameters.get("url");
        final String username = parameters.get("username");
        final String password = parameters.get("password");
        final String database = parameters.get("database");
        final String table    = parameters.get("table", "map_evolution");
        final String build    = parameters.get("build", "A");     // A or B
        final int records     = parameters.getInt("records", 100);
        final int idStart     = parameters.getInt("idStart", 0);

        ClickHouseClientConfig clientConfig = new ClickHouseClientConfig(
                url, username, password, database, table);

        boolean isBuildB = "B".equalsIgnoreCase(build);
        DataMapper<Event> mapper = isBuildB
                ? new MapEvolutionMapperB()
                : new MapEvolutionMapperA();
        ClickHouseConvertor<Event> convertor = new ClickHouseConvertor<>(Event.class, mapper);

        ClickHouseAsyncSink<Event> sink = ClickHouseAsyncSink.<Event>builder()
                .setElementConverter(convertor)
                .setMaxBatchSize(MAX_BATCH_SIZE)
                .setMaxInFlightRequests(MAX_IN_FLIGHT_REQUESTS)
                .setMaxBufferedRequests(MAX_BUFFERED_REQUESTS)
                .setMaxBatchSizeInBytes(MAX_BATCH_SIZE_IN_BYTES)
                .setMaxTimeInBufferMS(isBuildB ? BUILD_B_BUFFER_MS : BUILD_A_BUFFER_MS)
                .setMaxRecordSizeInBytes(MAX_RECORD_SIZE_IN_BYTES)
                .setClickHouseClientConfig(clientConfig)
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2_000);

        List<Event> events = new ArrayList<>(records);
        for (int i = idStart; i < idStart + records; i++) {
            events.add(isBuildB
                    ? new Event(i, "name-" + i, "extra-" + i)
                    : new Event(i, "name-" + i));
        }

        // IdleAfterEmitSource emits the records once then idles, keeping the job
        // alive so the upgrade workflow can take a savepoint. A bounded source
        // (e.g. fromSequence) would trigger endOfInput on the sink and force a
        // flush before the savepoint.
        env.addSource(new IdleAfterEmitSource<>(events))
                .returns(Event.class)
                .uid("map-evolution-source").name("map-evolution-source")
                .sinkTo(sink).uid("clickhouse-sink").name("clickhouse-sink");

        env.execute("map-evolution build=" + build);
    }
}
