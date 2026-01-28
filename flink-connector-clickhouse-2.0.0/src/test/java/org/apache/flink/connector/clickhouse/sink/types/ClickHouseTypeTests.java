package org.apache.flink.connector.clickhouse.sink.types;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.convertor.POJOConvertor;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.connector.clickhouse.sink.convertor.DateTimePOJOConvertor;
import org.apache.flink.connector.clickhouse.sink.convertor.SimplePOJOConvertor;
import org.apache.flink.connector.clickhouse.sink.convertor.SimplePOJOWithDefaultsConvertor;
import org.apache.flink.connector.clickhouse.sink.pojo.DateTimePOJO;
import org.apache.flink.connector.clickhouse.sink.pojo.SimplePOJO;
import org.apache.flink.connector.clickhouse.sink.pojo.SimplePOJOWithDefaults;
import org.apache.flink.connector.test.FlinkClusterTests;
import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.connector.clickhouse.sink.ClickHouseSinkTestUtils.*;
import static org.apache.flink.connector.test.embedded.flink.EmbeddedFlinkClusterForTests.executeAsyncJob;

public class ClickHouseTypeTests extends FlinkClusterTests {
    static final int STREAM_PARALLELISM = 5;
    static final int EXPECTED_ROWS = 10000;

    private static Stream<Arguments> provideTypeAndPojoList() {
        return Stream.of(
                Arguments.of("DateTime",
                        List.of(
                                new DateTimePOJO("user-001", Instant.parse("2024-01-15T10:30:00Z"), DateTimePOJO.DataType.DATETIME, 0, 42),
                                new DateTimePOJO("user-002", Instant.parse("2024-02-20T14:15:30Z"), DateTimePOJO.DataType.DATETIME, 0, 158),
                                new DateTimePOJO("user-003", Instant.parse("2024-03-10T08:45:12Z"), DateTimePOJO.DataType.DATETIME, 0, 7))),
                Arguments.of("DateTime64(3)",
                        List.of(
                                new DateTimePOJO("user-001", Instant.parse("2024-01-15T10:30:00.123Z"), DateTimePOJO.DataType.DATETIME64, 3, 42),
                                new DateTimePOJO("user-002", Instant.parse("2024-02-20T14:15:30.456Z"), DateTimePOJO.DataType.DATETIME64, 3, 158),
                                new DateTimePOJO("user-003", Instant.parse("2024-03-10T08:45:12.789Z"), DateTimePOJO.DataType.DATETIME64, 3, 7))),
                Arguments.of("DateTime64(6)",
                        List.of(
                                new DateTimePOJO("user-001", Instant.parse("2024-01-15T10:30:00.000000Z"), DateTimePOJO.DataType.DATETIME64, 6, 42),
                                new DateTimePOJO("user-002", Instant.parse("2024-02-20T14:15:30.301123Z"), DateTimePOJO.DataType.DATETIME64, 6, 158),
                                new DateTimePOJO("user-003", Instant.parse("2024-03-10T08:45:12.121234Z"), DateTimePOJO.DataType.DATETIME64, 6, 7))),
                Arguments.of("DateTime64(9)",
                        List.of(
                                new DateTimePOJO("user-001", Instant.parse("2024-01-15T10:30:00.000000000Z"), DateTimePOJO.DataType.DATETIME64, 9, 42),
                                new DateTimePOJO("user-002", Instant.parse("2024-02-20T14:15:30.301123321Z"), DateTimePOJO.DataType.DATETIME64, 9, 158),
                                new DateTimePOJO("user-003", Instant.parse("2024-03-10T08:45:12.121234556Z"), DateTimePOJO.DataType.DATETIME64, 9, 7)))
        );
    }

    @Test
    void testSimplePOJOTypes() throws Exception {
        String tableName = "simple_pojo";

        String dropTable = String.format("DROP TABLE IF EXISTS `%s`.`%s`", getDatabase(), tableName);
        ClickHouseServerForTests.executeSql(dropTable);
        // create table
        String tableSql = createSimplePOJOTableSQL(getDatabase(),  tableName);
        ClickHouseServerForTests.executeSql(tableSql);


        TableSchema simpleTableSchema = ClickHouseServerForTests.getTableSchema(tableName);
        POJOConvertor<SimplePOJO> simplePOJOConvertor = new SimplePOJOConvertor(simpleTableSchema.hasDefaults());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(STREAM_PARALLELISM);

        ClickHouseClientConfig clickHouseClientConfig = new ClickHouseClientConfig(getServerURL(), getUsername(), getPassword(), getDatabase(), tableName);
        clickHouseClientConfig.setSupportDefault(simpleTableSchema.hasDefaults());

        ElementConverter<SimplePOJO, ClickHousePayload> convertorCovid = new ClickHouseConvertor<>(SimplePOJO.class, simplePOJOConvertor);

        ClickHouseAsyncSink<SimplePOJO> simplePOJOSink = new ClickHouseAsyncSink<>(
                convertorCovid,
                MAX_BATCH_SIZE,
                MAX_IN_FLIGHT_REQUESTS,
                MAX_BUFFERED_REQUESTS,
                MAX_BATCH_SIZE_IN_BYTES,
                MAX_TIME_IN_BUFFER_MS,
                MAX_RECORD_SIZE_IN_BYTES,
                clickHouseClientConfig
        );

        List<SimplePOJO> simplePOJOList = new ArrayList<>();
        for (int i = 0; i < EXPECTED_ROWS; i++) {
            simplePOJOList.add(new SimplePOJO(i));
        }
        // create from list
        DataStream<SimplePOJO> simplePOJOs = env.fromElements(simplePOJOList.toArray(new SimplePOJO[0]));
        // send to a sink
        simplePOJOs.sinkTo(simplePOJOSink);
        int rows = executeAsyncJob(env, tableName, 10, EXPECTED_ROWS);
        Assertions.assertEquals(EXPECTED_ROWS, rows);
//        ClickHouseServerForTests.showData(tableName);
    }

    @ParameterizedTest
    @MethodSource("provideTypeAndPojoList")
    void testDateTime(String type, List<DateTimePOJO> simplePOJOList) throws Exception {
        String tableName = "simple_pojo_with_datetime";

        String dropTable = String.format("DROP TABLE IF EXISTS `%s`.`%s`", FlinkClusterTests.getDatabase(), tableName);
        ClickHouseServerForTests.executeSql(dropTable);
        // create table
        String tableSql = "CREATE TABLE `" + FlinkClusterTests.getDatabase() + "`.`" + tableName + "` (" +
                "id String," +
                String.format("created_at %s,", type) +
                "num_logins Int32," +
                ") " +
                "ENGINE = MergeTree " +
                "ORDER BY (id); ";
        ClickHouseServerForTests.executeSql(tableSql);

        TableSchema simpleTableSchema = ClickHouseServerForTests.getTableSchema(tableName);
        POJOConvertor<DateTimePOJO> simplePOJOWithDateTimeConvertor = new DateTimePOJOConvertor(simpleTableSchema.hasDefaults());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(STREAM_PARALLELISM);

        ClickHouseClientConfig clickHouseClientConfig = new ClickHouseClientConfig(FlinkClusterTests.getServerURL(), FlinkClusterTests.getUsername(), FlinkClusterTests.getPassword(), FlinkClusterTests.getDatabase(), tableName);
        clickHouseClientConfig.setSupportDefault(simpleTableSchema.hasDefaults());

        ElementConverter<DateTimePOJO, ClickHousePayload> convertorSimplePOJOWithDateTimeConvertor = new ClickHouseConvertor<>(DateTimePOJO.class, simplePOJOWithDateTimeConvertor);

        ClickHouseAsyncSink<DateTimePOJO> simplePOJOSink = new ClickHouseAsyncSink<>(
                convertorSimplePOJOWithDateTimeConvertor,
                MAX_BATCH_SIZE,
                MAX_IN_FLIGHT_REQUESTS,
                MAX_BUFFERED_REQUESTS,
                MAX_BATCH_SIZE_IN_BYTES,
                MAX_TIME_IN_BUFFER_MS,
                MAX_RECORD_SIZE_IN_BYTES,
                clickHouseClientConfig
        );

        // create from list
        DataStream<DateTimePOJO> simplePOJOs = env.fromCollection(simplePOJOList, TypeInformation.of(DateTimePOJO.class));
        // send to sink
        simplePOJOs.sinkTo(simplePOJOSink);
        int rows = executeAsyncJob(env, tableName, 10, 3);
        Assertions.assertEquals(3, rows);
        List<GenericRecord> genericRecordList = ClickHouseServerForTests.extractData(FlinkClusterTests.getDatabase(), tableName, "id");

        for (int j = 0; j < genericRecordList.size(); j++) {
            String id = simplePOJOList.get(j).id;
            Instant instant = simplePOJOList.get(j).createdAt;
            int numLogins = simplePOJOList.get(j).numLogins;

            Assertions.assertEquals(id, genericRecordList.get(j).getString("id"));
            Assertions.assertEquals(instant, genericRecordList.get(j).getZonedDateTime("created_at").toInstant());
            Assertions.assertEquals(numLogins, genericRecordList.get(j).getInteger("num_logins"));
        }
    }

    @Test
    void testSimplePOJOWithDefaultsTypes() throws Exception {
        String tableName = "simple_pojo_with_defaults";

        String dropTable = String.format("DROP TABLE IF EXISTS `%s`.`%s`", getDatabase(), tableName);
        ClickHouseServerForTests.executeSql(dropTable);
        // create table
        String tableSql = "CREATE TABLE `" + FlinkClusterTests.getDatabase() + "`.`" + tableName + "` (" +
                "id String," +
                "created_on DateTime64 DEFAULT now('UTC')" +
                ") " +
                "ENGINE = MergeTree " +
                "ORDER BY (id); ";
        ClickHouseServerForTests.executeSql(tableSql);


        TableSchema simpleTableSchema = ClickHouseServerForTests.getTableSchema(tableName);
        POJOConvertor<SimplePOJOWithDefaults> simplePOJOConvertor = new SimplePOJOWithDefaultsConvertor(simpleTableSchema.hasDefaults());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(STREAM_PARALLELISM);

        ClickHouseClientConfig clickHouseClientConfig = new ClickHouseClientConfig(getServerURL(), getUsername(), getPassword(), getDatabase(), tableName);
        clickHouseClientConfig.setSupportDefault(simpleTableSchema.hasDefaults());

        ElementConverter<SimplePOJOWithDefaults, ClickHousePayload> convertorCovid = new ClickHouseConvertor<>(SimplePOJOWithDefaults.class, simplePOJOConvertor);

        ClickHouseAsyncSink<SimplePOJOWithDefaults> simplePOJOWithDefaultsSink = new ClickHouseAsyncSink<>(
                convertorCovid,
                MAX_BATCH_SIZE,
                MAX_IN_FLIGHT_REQUESTS,
                MAX_BUFFERED_REQUESTS,
                MAX_BATCH_SIZE_IN_BYTES,
                MAX_TIME_IN_BUFFER_MS,
                MAX_RECORD_SIZE_IN_BYTES,
                clickHouseClientConfig
        );

        List<SimplePOJOWithDefaults> simplePOJOList = new ArrayList<>();
        for (int i = 0; i < EXPECTED_ROWS; i++) {
            simplePOJOList.add(new SimplePOJOWithDefaults(i));
        }
        // create from list
        DataStream<SimplePOJOWithDefaults> simplePOJOs = env.fromElements(simplePOJOList.toArray(new SimplePOJOWithDefaults[0]));
        // send to a sink
        simplePOJOs.sinkTo(simplePOJOWithDefaultsSink);
        int rows = executeAsyncJob(env, tableName, 10, EXPECTED_ROWS);
        Assertions.assertEquals(EXPECTED_ROWS, rows);

    }

}
