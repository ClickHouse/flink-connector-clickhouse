package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.data.ClickHouseFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.convertor.POJOConvertor;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.connector.clickhouse.sink.convertor.CovidPOJOConvertor;
import org.apache.flink.connector.clickhouse.sink.convertor.SimplePOJOConvertor;
import org.apache.flink.connector.clickhouse.sink.convertor.SimplePOJOWithJSONConvertor;
import org.apache.flink.connector.clickhouse.sink.pojo.CovidPOJO;
import org.apache.flink.connector.clickhouse.sink.pojo.SimplePOJO;
import org.apache.flink.connector.clickhouse.sink.pojo.SimplePOJOWithJSON;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.test.FlinkClusterTests;
import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseTestHelpers;
import org.apache.flink.connector.test.embedded.flink.EmbeddedFlinkClusterForTests;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests.*;
import static org.apache.flink.connector.clickhouse.sink.ClickHouseSinkTestUtils.*;
import static org.apache.flink.connector.test.embedded.flink.EmbeddedFlinkClusterForTests.executeAsyncJob;

public class ClickHouseSinkTests extends FlinkClusterTests {

    static final int EXPECTED_ROWS = 10000;
    static final int EXPECTED_ROWS_ON_FAILURE = 0;
    static final int STREAM_PARALLELISM = 5;
    static final int NUMBER_OF_RETRIES = 20;

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
        env.setParallelism(STREAM_PARALLELISM);

        ClickHouseClientConfig clickHouseClientConfig = new ClickHouseClientConfig(getServerURL(), getUsername(), getPassword(), getDatabase(), tableName);
        ElementConverter<String, ClickHousePayload> convertorString = new ClickHouseConvertor<>(String.class);
        // create sink
        ClickHouseAsyncSink<String> csvSink = new ClickHouseAsyncSink<>(
                convertorString,
                MAX_BATCH_SIZE,
                MAX_IN_FLIGHT_REQUESTS,
                MAX_BUFFERED_REQUESTS,
                MAX_BATCH_SIZE_IN_BYTES,
                MAX_TIME_IN_BUFFER_MS,
                MAX_RECORD_SIZE_IN_BYTES,
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
        int rows = executeAsyncJob(env, tableName, 10, EXPECTED_ROWS);
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

        POJOConvertor<CovidPOJO> covidPOJOConvertor = new CovidPOJOConvertor();
        final StreamExecutionEnvironment env = EmbeddedFlinkClusterForTests.getMiniCluster().getTestStreamEnvironment();
        env.setParallelism(STREAM_PARALLELISM);

        ClickHouseClientConfig clickHouseClientConfig = new ClickHouseClientConfig(getServerURL(), getUsername(), getPassword(), getDatabase(), tableName);
        clickHouseClientConfig.setSupportDefault(covidTableSchema.hasDefaults());
        ElementConverter<CovidPOJO, ClickHousePayload> convertorCovid = new ClickHouseConvertor<>(CovidPOJO.class, covidPOJOConvertor);

        ClickHouseAsyncSink<CovidPOJO> covidPOJOSink = new ClickHouseAsyncSink<>(
                convertorCovid,
                MAX_BATCH_SIZE,
                MAX_IN_FLIGHT_REQUESTS,
                MAX_BUFFERED_REQUESTS,
                MAX_BATCH_SIZE_IN_BYTES,
                MAX_TIME_IN_BUFFER_MS,
                MAX_RECORD_SIZE_IN_BYTES,
                clickHouseClientConfig
        );

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
        int rows = executeAsyncJob(env, tableName, 10, EXPECTED_ROWS);
        Assertions.assertEquals(EXPECTED_ROWS, rows);
    }

    @Test
    void ProductNameTest() throws Exception {
        String tableName = "product_name_csv_covid";
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
                MAX_BATCH_SIZE,
                MAX_IN_FLIGHT_REQUESTS,
                MAX_BUFFERED_REQUESTS,
                MAX_BATCH_SIZE_IN_BYTES,
                MAX_TIME_IN_BUFFER_MS,
                MAX_RECORD_SIZE_IN_BYTES,
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
        int rows = executeAsyncJob(env, tableName, 10, EXPECTED_ROWS);
        Assertions.assertEquals(EXPECTED_ROWS, rows);
        if (ClickHouseServerForTests.isCloud())
            ClickHouseServerForTests.executeSql("SYSTEM FLUSH LOGS ON CLUSTER 'default'");
        else
            ClickHouseServerForTests.executeSql("SYSTEM FLUSH LOGS");

        if (ClickHouseServerForTests.isCloud())
            Thread.sleep(10000);
        // let's wait until data will be available in query log
        String startWith = String.format("Flink-ClickHouse-Sink/%s", ClickHouseSinkVersion.getVersion());
        String productName = ClickHouseServerForTests.extractProductName(ClickHouseServerForTests.getDatabase(), tableName, startWith);
        String compareString = String.format("Flink-ClickHouse-Sink/%s (fv:flink/2.0.0, lv:scala/2.12)", ClickHouseSinkVersion.getVersion());
        boolean isContains = productName.contains(compareString);
        Assertions.assertTrue(isContains, "Expected user agent to contain: " + compareString + " but got: " + productName);
    }

    /**
     * Suppose to drop data on failure. The way we try to generate this use case is by supplying the writer with wrong Format
     * @throws Exception
     */
    @Test
    void CSVDataOnFailureDropDataTest() throws Exception {
        String tableName = "csv_failure_covid";
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
        env.setParallelism(STREAM_PARALLELISM);

        ClickHouseClientConfig clickHouseClientConfig = new ClickHouseClientConfig(getServerURL(), getUsername(), getPassword(), getDatabase(), tableName);
        ElementConverter<String, ClickHousePayload> convertorString = new ClickHouseConvertor<>(String.class);
        // create sink
        ClickHouseAsyncSink<String> csvSink = new ClickHouseAsyncSink<>(
                convertorString,
                MAX_BATCH_SIZE,
                MAX_IN_FLIGHT_REQUESTS,
                MAX_BUFFERED_REQUESTS,
                MAX_BATCH_SIZE_IN_BYTES,
                MAX_TIME_IN_BUFFER_MS,
                MAX_RECORD_SIZE_IN_BYTES,
                clickHouseClientConfig
        );
        csvSink.setClickHouseFormat(ClickHouseFormat.TSV);

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
        // TODO: make the test smarter by checking the counter of numOfDroppedRecords equals EXPECTED_ROWS
        int rows = executeAsyncJob(env, tableName, 10, EXPECTED_ROWS);
        Assertions.assertEquals(EXPECTED_ROWS_ON_FAILURE, rows);
    }

    /**
     * Suppose to retry and drop data on failure. The way we try to generate this use case is by supplying a different port of ClickHouse server
     * @throws Exception
     */
    @Test
    void CSVDataOnRetryAndDropDataTest() throws Exception {
        String tableName = "csv_retry_covid";
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
        env.setParallelism(STREAM_PARALLELISM);


        ClickHouseClientConfig clickHouseClientConfig = new ClickHouseClientConfig(getServerURL(), getUsername(), getPassword(), getDatabase(), tableName);
        ElementConverter<String, ClickHousePayload> convertorString = new ClickHouseConvertor<>(String.class);
        // create sink
        ClickHouseAsyncSink<String> csvSink = new ClickHouseAsyncSink<>(
                convertorString,
                MAX_BATCH_SIZE,
                MAX_IN_FLIGHT_REQUESTS,
                MAX_BUFFERED_REQUESTS,
                MAX_BATCH_SIZE_IN_BYTES,
                MAX_TIME_IN_BUFFER_MS,
                MAX_RECORD_SIZE_IN_BYTES,
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
        lines.map(line -> line + ", error, error").sinkTo(csvSink);
        // TODO: make the test smarter by checking the counter of numOfDroppedRecords equals EXPECTED_ROWS
        int rows = executeAsyncJob(env, tableName, 10, EXPECTED_ROWS);
        Assertions.assertEquals(EXPECTED_ROWS_ON_FAILURE, rows);
    }

    /*
        In this test, we lower the parts_to_throw_insert setting (https://clickhouse.com/docs/operations/settings/merge-tree-settings#parts_to_throw_insert) to trigger the "Too Many Parts" error more easily.
        Once we exceed this threshold, ClickHouse will reject INSERT operations with a "Too Many Parts" error.
        Our retry implementation will demonstrate how it handles these failures by retrying the inserts until all rows are successfully inserted. We will insert one batch containing two records to observe this behavior.
    */
    @Test
    void SimplePOJODataTooManyPartsTest() throws Exception {
        // this test is not running on cloud
        if (isCloud())
            return;
        String tableName = "simple_too_many_parts_pojo";

        String dropTable = String.format("DROP TABLE IF EXISTS `%s`.`%s`", getDatabase(), tableName);
        ClickHouseServerForTests.executeSql(dropTable);
        // create table
        String tableSql = createSimplePOJOTableSQL(getDatabase(),  tableName, 10);
        ClickHouseServerForTests.executeSql(tableSql);
        //ClickHouseServerForTests.executeSql(String.format("SYSTEM STOP MERGES `%s.%s`", getDatabase(), tableName));

        TableSchema simpleTableSchema = ClickHouseServerForTests.getTableSchema(tableName);
        POJOConvertor<SimplePOJO> simplePOJOConvertor = new SimplePOJOConvertor();

        final StreamExecutionEnvironment env = EmbeddedFlinkClusterForTests.getMiniCluster().getTestStreamEnvironment();
        env.setParallelism(STREAM_PARALLELISM);

        ClickHouseClientConfig clickHouseClientConfig = new ClickHouseClientConfig(getServerURL(), getUsername(), getPassword(), getDatabase(), tableName);
        clickHouseClientConfig.setNumberOfRetries(NUMBER_OF_RETRIES);
        clickHouseClientConfig.setSupportDefault(simpleTableSchema.hasDefaults());

        ElementConverter<SimplePOJO, ClickHousePayload> convertorCovid = new ClickHouseConvertor<>(SimplePOJO.class, simplePOJOConvertor);

        ClickHouseAsyncSink<SimplePOJO> simplePOJOSink = new ClickHouseAsyncSink<>(
                convertorCovid,
                MIN_BATCH_SIZE * 2,
                MAX_IN_FLIGHT_REQUESTS,
                10,
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
        DataStream<SimplePOJO> simplePOJOs = env.fromData(simplePOJOList.toArray(new SimplePOJO[0]));
        // send to a sink
        simplePOJOs.sinkTo(simplePOJOSink);
        int rows = executeAsyncJob(env, tableName, 100, EXPECTED_ROWS);
        Assertions.assertEquals(EXPECTED_ROWS, rows);
        //ClickHouseServerForTests.executeSql(String.format("SYSTEM START MERGES `%s.%s`", getDatabase(), tableName));
    }

    @Test
    void CheckClickHouseAlive() {
        Assertions.assertThrows(RuntimeException.class, () -> { new ClickHouseClientConfig(getServerURL(), getUsername() + "wrong_username", getPassword(), getDatabase(), "dummy");});
    }

    @Test
    void SimplePOJOWithJSONDataTest() throws Exception {
        Assumptions.assumeTrue(
                isCloud() || ClickHouseTestHelpers.getClickhouseVersion().equalsIgnoreCase("latest"));
        String tableName = "simple_pojo_with_json_data";

        String dropTable = String.format("DROP TABLE IF EXISTS `%s`.`%s`", getDatabase(), tableName);
        ClickHouseServerForTests.executeSql(dropTable);
        // create table
        String tableSql = "CREATE TABLE `" + getDatabase() + "`.`" + tableName + "` (" +
                "longPrimitive Int64," +
                "jsonPayload JSON," +
                ") " +
                "ENGINE = MergeTree " +
                "ORDER BY (longPrimitive); ";
        ClickHouseServerForTests.executeSql(tableSql);

        TableSchema simplePOJOWithJSONTableSchema = ClickHouseServerForTests.getTableSchema(tableName);

        POJOConvertor<SimplePOJOWithJSON> simplePOJOWithJSONConvertor = new SimplePOJOWithJSONConvertor();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(STREAM_PARALLELISM);

        ClickHouseClientConfig clickHouseClientConfig = new ClickHouseClientConfig(getServerURL(), getUsername(), getPassword(), getDatabase(), tableName, true);
        clickHouseClientConfig.setSupportDefault(simplePOJOWithJSONTableSchema.hasDefaults());
        ElementConverter<SimplePOJOWithJSON, ClickHousePayload> convertorCovid = new ClickHouseConvertor<>(SimplePOJOWithJSON.class, simplePOJOWithJSONConvertor);

        ClickHouseAsyncSink<SimplePOJOWithJSON> simplePOJOWithJSONSink = new ClickHouseAsyncSink<>(
                convertorCovid,
                MAX_BATCH_SIZE,
                MAX_IN_FLIGHT_REQUESTS,
                MAX_BUFFERED_REQUESTS,
                MAX_BATCH_SIZE_IN_BYTES,
                MAX_TIME_IN_BUFFER_MS,
                MAX_RECORD_SIZE_IN_BYTES,
                clickHouseClientConfig
        );

        List<SimplePOJOWithJSON> simplePOJOWithJSONList = new ArrayList<>();
        for (int i = 0; i < EXPECTED_ROWS; i++) {
            simplePOJOWithJSONList.add(new SimplePOJOWithJSON(i));
        }
        // create from list
        DataStream<SimplePOJOWithJSON> simplePOJOsWithJSON = env.fromElements(simplePOJOWithJSONList.toArray(new SimplePOJOWithJSON[0]));
        // send to a sink
        simplePOJOsWithJSON.sinkTo(simplePOJOWithJSONSink);
        int rows = executeAsyncJob(env, tableName, 100, EXPECTED_ROWS);
        Assertions.assertEquals(EXPECTED_ROWS, rows);

        List<GenericRecord> genericRecordList = ClickHouseServerForTests.extractData(getDatabase(), tableName, "longPrimitive", "getSubcolumn(jsonPayload, 'bar') as bar");
        for (int j = 0; j < genericRecordList.size(); j++) {
            long longPrimitive = simplePOJOWithJSONList.get(j).getLongPrimitive();
            String foo = genericRecordList.get(j).getString("bar");
            Assertions.assertEquals(longPrimitive, genericRecordList.get(j).getLong("longPrimitive"));
            Assertions.assertEquals("foo", foo);
        }
    }
}
