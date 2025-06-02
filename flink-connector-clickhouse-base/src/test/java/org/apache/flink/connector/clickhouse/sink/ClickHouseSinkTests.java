package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.convertor.POJOConvertor;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.connector.clickhouse.sink.convertor.CovidPOJOConvertor;
import org.apache.flink.connector.clickhouse.sink.convertor.SimplePOJOConvertor;
import org.apache.flink.connector.clickhouse.sink.pojo.CovidPOJO;
import org.apache.flink.connector.clickhouse.sink.pojo.SimplePOJO;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.test.FlinkClusterTests;
import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.apache.flink.connector.test.embedded.flink.EmbeddedFlinkClusterForTests;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class ClickHouseSinkTests extends FlinkClusterTests {

    static final int EXPECTED_ROWS = 10000;

    private int executeJob(StreamExecutionEnvironment env, String tableName) throws Exception {
        JobClient jobClient = env.executeAsync("Read GZipped CSV with FileSource");
        int rows = 0;
        int iterations = 0;
        while (iterations < 10) {
            Thread.sleep(1000);
            iterations++;
            rows = ClickHouseServerForTests.countRows(tableName);
            System.out.println("Rows: " + rows);
            if (rows == EXPECTED_ROWS)
                break;
        }
        // cancel job
        jobClient.cancel();
        return rows;
    }

    @Test
    void CSVDataTest() throws Exception {
        String tableName = "csv_covid";
        String dropTable = String.format("DROP TABLE IF EXISTS `%s`.`%s`", getDatabase(), tableName);
        ClickHouseServerForTests.executeSql(dropTable);
        // create table
        String tableSql = "CREATE TABLE `" + getDatabase() + "`.`" + tableName + "` (" +
                            "date Date," +
                            "location_key LowCardinality(String)," +
                            "new_confirmed Int32," +
                            "new_deceased Int32," +
                            "new_recovered Int32," +
                            "new_tested Int32," +
                            "cumulative_confirmed Int32," +
                            "cumulative_deceased Int32," +
                            "cumulative_recovered Int32," +
                            "cumulative_tested Int32" +
                            ") " +
                            "ENGINE = MergeTree " +
                            "ORDER BY (location_key, date); ";
        ClickHouseServerForTests.executeSql(tableSql);

        final StreamExecutionEnvironment env = EmbeddedFlinkClusterForTests.getMiniCluster().getTestStreamEnvironment();
        env.setParallelism(1);

        ClickHouseClientConfig clickHouseClientConfig = new ClickHouseClientConfig(getServerURL(), getUsername(), getPassword(), getDatabase(), tableName);
        ElementConverter<String, ClickHousePayload> convertorString = new ClickHouseConvertor<>(String.class);
        // create sink
        ClickHouseAsyncSink<String> csvSink = new ClickHouseAsyncSink<>(
                convertorString,
                5000,
                2,
                20000,
                1024 * 1024,
                5 * 1000,
                1000,
                clickHouseClientConfig
        );
        csvSink.setClickHouseFormat(ClickHouseFormat.CSV);

        Path filePath = new Path("./src/test/resources/epidemiology_top_10000.csv.gz");

        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), filePath)
                .build();
        // read csv data from file
        DataStreamSource<String> lines = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "GzipCsvSource"
        );
        lines.sinkTo(csvSink);
        int rows = executeJob(env, tableName);
        Assertions.assertEquals(EXPECTED_ROWS, rows);
    }

    @Test
    void CovidPOJODataTest() throws Exception {
        String tableName = "covid_pojo";

        String dropTable = String.format("DROP TABLE IF EXISTS `%s`.`%s`", getDatabase(), tableName);
        ClickHouseServerForTests.executeSql(dropTable);
        // create table
        String tableSql = "CREATE TABLE `" + getDatabase() + "`.`" + tableName + "` (" +
                "date Date," +
                "location_key LowCardinality(String)," +
                "new_confirmed Int32," +
                "new_deceased Int32," +
                "new_recovered Int32," +
                "new_tested Int32," +
                "cumulative_confirmed Int32," +
                "cumulative_deceased Int32," +
                "cumulative_recovered Int32," +
                "cumulative_tested Int32" +
                ") " +
                "ENGINE = MergeTree " +
                "ORDER BY (location_key, date); ";
        ClickHouseServerForTests.executeSql(tableSql);


        TableSchema covidTableSchema = ClickHouseServerForTests.getTableSchema(tableName);
//        POJOConvertor<CovidPOJO> covidPOJOConvertor = POJOSerializable.create().createConvertor(covidTableSchema, CovidPOJO.class);

        POJOConvertor<CovidPOJO> covidPOJOConvertor = new CovidPOJOConvertor();
        final StreamExecutionEnvironment env = EmbeddedFlinkClusterForTests.getMiniCluster().getTestStreamEnvironment();
        env.setParallelism(5);

        ClickHouseClientConfig clickHouseClientConfig = new ClickHouseClientConfig(getServerURL(), getUsername(), getPassword(), getDatabase(), tableName);
        ElementConverter<CovidPOJO, ClickHousePayload> convertorCovid = new ClickHouseConvertor<>(CovidPOJO.class, covidPOJOConvertor);

        ClickHouseAsyncSink<CovidPOJO> covidPOJOSink = new ClickHouseAsyncSink<>(
                convertorCovid,
                5000,
                2,
                20000,
                1024 * 1024,
                5 * 1000,
                1000,
                clickHouseClientConfig
        );

        covidPOJOSink.setClickHouseFormat(ClickHouseFormat.RowBinary);

        Path filePath = new Path("./src/test/resources/epidemiology_top_10000.csv.gz");

        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), filePath)
                .build();
        // read csv data from file
        DataStreamSource<String> lines = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "GzipCsvSource"
        );

        // convert to POJO
        DataStream<CovidPOJO> covidPOJOs = lines.map(new MapFunction<String, CovidPOJO>() {
            @Override
            public CovidPOJO map(String value) throws Exception {
                return new CovidPOJO(value);
            }
        });
        // send to a sink
        covidPOJOs.sinkTo(covidPOJOSink);
        int rows = executeJob(env, tableName);
        Assertions.assertEquals(EXPECTED_ROWS, rows);
    }

    @Test
    void SimplePOJODataTest() throws Exception {
        // TODO: needs to be extended to all types
        String tableName = "simple_pojo";

        String dropTable = String.format("DROP TABLE IF EXISTS `%s`.`%s`", getDatabase(), tableName);
        ClickHouseServerForTests.executeSql(dropTable);
        // create table
        String tableSql = "CREATE TABLE `" + getDatabase() + "`.`" + tableName + "` (" +
                "bytePrimitive Int8," +
                "byteObject Int8," +
                "shortPrimitive Int16," +
                "shortObject Int16," +
                "intPrimitive Int32," +
                "integerObject Int32," +
                "longPrimitive Int64," +
                "longObject Int64," +
                "floatPrimitive Float," +
                "floatObject Float," +
                "doublePrimitive Double," +
                "doubleObject Double," +
                ") " +
                "ENGINE = MergeTree " +
                "ORDER BY (longPrimitive); ";
        ClickHouseServerForTests.executeSql(tableSql);


        TableSchema simpleTableSchema = ClickHouseServerForTests.getTableSchema(tableName);
//        POJOConvertor<SimplePOJO> simplePOJOConvertor = POJOSerializable.create().createConvertor(simpleTableSchema, SimplePOJO.class);
        POJOConvertor<SimplePOJO> simplePOJOConvertor = new SimplePOJOConvertor();

        final StreamExecutionEnvironment env = EmbeddedFlinkClusterForTests.getMiniCluster().getTestStreamEnvironment();
        env.setParallelism(5);

        ClickHouseClientConfig clickHouseClientConfig = new ClickHouseClientConfig(getServerURL(), getUsername(), getPassword(), getDatabase(), tableName);
        ElementConverter<SimplePOJO, ClickHousePayload> convertorCovid = new ClickHouseConvertor<>(SimplePOJO.class, simplePOJOConvertor);

        ClickHouseAsyncSink<SimplePOJO> simplePOJOSink = new ClickHouseAsyncSink<>(
                convertorCovid,
                5000,
                2,
                20000,
                1024 * 1024,
                5 * 1000,
                1000,
                clickHouseClientConfig
        );

        simplePOJOSink.setClickHouseFormat(ClickHouseFormat.RowBinary);

        List<SimplePOJO> simplePOJOList = new ArrayList<>();
        for (int i = 0; i < EXPECTED_ROWS; i++) {
            simplePOJOList.add(new SimplePOJO(i));
        }
        // create from list
        DataStream<SimplePOJO> simplePOJOs = env.fromData(simplePOJOList.toArray(new SimplePOJO[0]));
        // send to a sink
        simplePOJOs.sinkTo(simplePOJOSink);
        int rows = executeJob(env, tableName);
        Assertions.assertEquals(EXPECTED_ROWS, rows);
    }
}
