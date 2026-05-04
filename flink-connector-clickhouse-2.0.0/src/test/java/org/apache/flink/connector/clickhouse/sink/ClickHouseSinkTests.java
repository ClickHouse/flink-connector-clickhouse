package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.config.RetryPolicy;
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
import org.apache.flink.connector.clickhouse.sink.source.FailingSource;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.test.FlinkClusterTests;
import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.apache.flink.connector.test.embedded.flink.EmbeddedFlinkClusterForTests;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

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
        ClickHouseConvertor<String> convertorString = new ClickHouseConvertor<>(String.class);
        // create sink
        ClickHouseAsyncSink<String> csvSink = ClickHouseAsyncSink.<String>builder()
                .setElementConverter(convertorString)
                .setMaxBatchSize(MAX_BATCH_SIZE)
                .setMaxInFlightRequests(MAX_IN_FLIGHT_REQUESTS)
                .setMaxBufferedRequests(MAX_BUFFERED_REQUESTS)
                .setMaxBatchSizeInBytes(MAX_BATCH_SIZE_IN_BYTES)
                .setMaxTimeInBufferMS(MAX_TIME_IN_BUFFER_MS)
                .setMaxRecordSizeInBytes(MAX_RECORD_SIZE_IN_BYTES)
                .setClickHouseClientConfig(clickHouseClientConfig)
                .setClickHouseFormat(ClickHouseFormat.CSV)
                .build();

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

        POJOConvertor<CovidPOJO> covidPOJOConvertor = new CovidPOJOConvertor(covidTableSchema.hasDefaults());
        final StreamExecutionEnvironment env = EmbeddedFlinkClusterForTests.getMiniCluster().getTestStreamEnvironment();
        env.setParallelism(STREAM_PARALLELISM);

        ClickHouseClientConfig clickHouseClientConfig = new ClickHouseClientConfig(getServerURL(), getUsername(), getPassword(), getDatabase(), tableName);
        clickHouseClientConfig.setSupportDefault(covidTableSchema.hasDefaults());
        ClickHouseConvertor<CovidPOJO> convertorCovid = new ClickHouseConvertor<>(CovidPOJO.class, covidPOJOConvertor);

        ClickHouseAsyncSink<CovidPOJO> covidPOJOSink = ClickHouseAsyncSink.<CovidPOJO>builder()
                .setElementConverter(convertorCovid)
                .setMaxBatchSize(MAX_BATCH_SIZE)
                .setMaxInFlightRequests(MAX_IN_FLIGHT_REQUESTS)
                .setMaxBufferedRequests(MAX_BUFFERED_REQUESTS)
                .setMaxBatchSizeInBytes(MAX_BATCH_SIZE_IN_BYTES)
                .setMaxTimeInBufferMS(MAX_TIME_IN_BUFFER_MS)
                .setMaxRecordSizeInBytes(MAX_RECORD_SIZE_IN_BYTES)
                .setClickHouseClientConfig(clickHouseClientConfig)
                .build();

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
        ClickHouseConvertor<String> convertorString = new ClickHouseConvertor<>(String.class);
        // create sink
        ClickHouseAsyncSink<String> csvSink = ClickHouseAsyncSink.<String>builder()
                .setElementConverter(convertorString)
                .setMaxBatchSize(MAX_BATCH_SIZE)
                .setMaxInFlightRequests(MAX_IN_FLIGHT_REQUESTS)
                .setMaxBufferedRequests(MAX_BUFFERED_REQUESTS)
                .setMaxBatchSizeInBytes(MAX_BATCH_SIZE_IN_BYTES)
                .setMaxTimeInBufferMS(MAX_TIME_IN_BUFFER_MS)
                .setMaxRecordSizeInBytes(MAX_RECORD_SIZE_IN_BYTES)
                .setClickHouseClientConfig(clickHouseClientConfig)
                .setClickHouseFormat(ClickHouseFormat.CSV)
                .build();

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
     *
     * @throws Exception
     */
    @Test
    void CSVDataOnFailureDropDataTest() throws Exception {
        String tableName = "csv_failure_covid";
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
        ClickHouseConvertor<String> convertorString = new ClickHouseConvertor<>(String.class);
        // create sink
        ClickHouseAsyncSink<String> csvSink = ClickHouseAsyncSink.<String>builder()
                .setElementConverter(convertorString)
                .setMaxBatchSize(MAX_BATCH_SIZE)
                .setMaxInFlightRequests(MAX_IN_FLIGHT_REQUESTS)
                .setMaxBufferedRequests(MAX_BUFFERED_REQUESTS)
                .setMaxBatchSizeInBytes(MAX_BATCH_SIZE_IN_BYTES)
                .setMaxTimeInBufferMS(MAX_TIME_IN_BUFFER_MS)
                .setMaxRecordSizeInBytes(MAX_RECORD_SIZE_IN_BYTES)
                .setClickHouseClientConfig(clickHouseClientConfig)
                .setClickHouseFormat(ClickHouseFormat.TSV)
                .build();

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
     *
     * @throws Exception
     */
    @Test
    void CSVDataOnRetryAndDropDataTest() throws Exception {
        String tableName = "csv_retry_covid";
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
        ClickHouseConvertor<String> convertorString = new ClickHouseConvertor<>(String.class);
        // create sink
        ClickHouseAsyncSink<String> csvSink = ClickHouseAsyncSink.<String>builder()
                .setElementConverter(convertorString)
                .setMaxBatchSize(MAX_BATCH_SIZE)
                .setMaxInFlightRequests(MAX_IN_FLIGHT_REQUESTS)
                .setMaxBufferedRequests(MAX_BUFFERED_REQUESTS)
                .setMaxBatchSizeInBytes(MAX_BATCH_SIZE_IN_BYTES)
                .setMaxTimeInBufferMS(MAX_TIME_IN_BUFFER_MS)
                .setMaxRecordSizeInBytes(MAX_RECORD_SIZE_IN_BYTES)
                .setClickHouseClientConfig(clickHouseClientConfig)
                .setClickHouseFormat(ClickHouseFormat.CSV)
                .build();

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

        // create table
        String tableSql = SimplePOJO.createTableSQL(getDatabase(), tableName, 10);
        ClickHouseServerForTests.executeSql(tableSql);
        //ClickHouseServerForTests.executeSql(String.format("SYSTEM STOP MERGES `%s.%s`", getDatabase(), tableName));

        TableSchema simpleTableSchema = ClickHouseServerForTests.getTableSchema(tableName);
        POJOConvertor<SimplePOJO> simplePOJOConvertor = new SimplePOJOConvertor(simpleTableSchema.hasDefaults());

        final StreamExecutionEnvironment env = EmbeddedFlinkClusterForTests.getMiniCluster().getTestStreamEnvironment();
        env.setParallelism(STREAM_PARALLELISM);

        ClickHouseClientConfig clickHouseClientConfig = new ClickHouseClientConfig(getServerURL(), getUsername(), getPassword(), getDatabase(), tableName);
        // Retry forever: "Too Many Parts" needs background merges to clear, and the writer's
        // current retry path has no backoff — a limited retry budget gets exhausted in ms before
        // any merge runs. Tracked separately; see TODO add proper backoff to RetriableException
        // path in ClickHouseAsyncWriter.handleFailedRequest.
        clickHouseClientConfig.setRetryPolicy(RetryPolicy.forever());
        clickHouseClientConfig.setSupportDefault(simpleTableSchema.hasDefaults());

        ClickHouseConvertor<SimplePOJO> convertorCovid = new ClickHouseConvertor<>(SimplePOJO.class, simplePOJOConvertor);

        ClickHouseAsyncSink<SimplePOJO> simplePOJOSink = ClickHouseAsyncSink.<SimplePOJO>builder()
                .setElementConverter(convertorCovid)
                .setMaxBatchSize(MIN_BATCH_SIZE * 2)
                .setMaxInFlightRequests(MAX_IN_FLIGHT_REQUESTS)
                .setMaxBufferedRequests(10)
                .setMaxBatchSizeInBytes(MAX_BATCH_SIZE_IN_BYTES)
                .setMaxTimeInBufferMS(MAX_TIME_IN_BUFFER_MS)
                .setMaxRecordSizeInBytes(MAX_RECORD_SIZE_IN_BYTES)
                .setClickHouseClientConfig(clickHouseClientConfig)
                .build();

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
        Assertions.assertThrows(RuntimeException.class, () -> {
            new ClickHouseClientConfig(getServerURL(), getUsername() + "wrong_username", getPassword(), getDatabase(), "dummy");
        });
    }

    @Test
    void DataCorruptionCovidTest() throws Exception {
        String tableName = "covid_corruption_test";

        // create table
        ClickHouseServerForTests.executeSql(CovidPOJO.createTableSql(getDatabase(), tableName));

        TableSchema covidTableSchema = ClickHouseServerForTests.getTableSchema(tableName);

        POJOConvertor<CovidPOJO> covidPOJOConvertor = new CovidPOJOConvertor(covidTableSchema.hasDefaults());
        final StreamExecutionEnvironment env = EmbeddedFlinkClusterForTests.getMiniCluster().getTestStreamEnvironment();
        env.setParallelism(STREAM_PARALLELISM);

        ClickHouseClientConfig clickHouseClientConfig = new ClickHouseClientConfig(getServerURL(), getUsername(), getPassword(), getDatabase(), tableName);
        clickHouseClientConfig.setSupportDefault(covidTableSchema.hasDefaults());
        ClickHouseConvertor<CovidPOJO> convertorCovid = new ClickHouseConvertor<>(CovidPOJO.class, covidPOJOConvertor);

        ClickHouseAsyncSink<CovidPOJO> covidPOJOSink = ClickHouseAsyncSink.<CovidPOJO>builder()
                .setElementConverter(convertorCovid)
                .setMaxBatchSize(MAX_BATCH_SIZE)
                .setMaxInFlightRequests(MAX_IN_FLIGHT_REQUESTS)
                .setMaxBufferedRequests(MAX_BUFFERED_REQUESTS)
                .setMaxBatchSizeInBytes(MAX_BATCH_SIZE_IN_BYTES)
                .setMaxTimeInBufferMS(MAX_TIME_IN_BUFFER_MS)
                .setMaxRecordSizeInBytes(MAX_RECORD_SIZE_IN_BYTES)
                .setClickHouseClientConfig(clickHouseClientConfig)
                .build();

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

        // First batch: insert only first MAX_BATCH_SIZE records
        DataStream<CovidPOJO> firstBatch = lines.map(new MapFunction<String, CovidPOJO>() {
            private int recordCount = 0;
            
            @Override
            public CovidPOJO map(String value) throws Exception {
                recordCount++;
                // Only process first MAX_BATCH_SIZE records
                if (recordCount <= MAX_BATCH_SIZE) {
                    return new CovidPOJO(value);
                }
                // Return null for records we don't want to process in this batch
                return null;
            }
        }).setParallelism(1).filter(record -> record != null); // Filter out null records
        
        // send first batch to sink
        firstBatch.sinkTo(covidPOJOSink);
        
        // Execute first batch and verify it succeeds
        int firstBatchRows = executeAsyncJob(env, tableName, 10, MAX_BATCH_SIZE);
        Assertions.assertEquals(MAX_BATCH_SIZE, firstBatchRows, 
            "Expected exactly " + MAX_BATCH_SIZE + " rows from first batch, but got: " + firstBatchRows);
        
        // Now change the table schema to simulate corruption/incompatibility
        // Drop a critical column that the POJO expects to exist
        String alterTableSql = "ALTER TABLE `" + getDatabase() + "`.`" + tableName + "` " +
                "DROP COLUMN new_confirmed";
        ClickHouseServerForTests.executeSql(alterTableSql);
        
        // Try to insert the remaining records with a new sink
        // This should fail due to schema mismatch
        ClickHouseAsyncSink<CovidPOJO> secondBatchSink = ClickHouseAsyncSink.<CovidPOJO>builder()
                .setElementConverter(convertorCovid)
                .setMaxBatchSize(MAX_BATCH_SIZE)
                .setMaxInFlightRequests(MAX_IN_FLIGHT_REQUESTS)
                .setMaxBufferedRequests(MAX_BUFFERED_REQUESTS)
                .setMaxBatchSizeInBytes(MAX_BATCH_SIZE_IN_BYTES)
                .setMaxTimeInBufferMS(MAX_TIME_IN_BUFFER_MS)
                .setMaxRecordSizeInBytes(MAX_RECORD_SIZE_IN_BYTES)
                .setClickHouseClientConfig(clickHouseClientConfig)
                .build();
        
        // Create new environment for second batch
        final StreamExecutionEnvironment env2 = EmbeddedFlinkClusterForTests.getMiniCluster().getTestStreamEnvironment();
        env2.setParallelism(STREAM_PARALLELISM);
        
        FileSource<String> source2 = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), filePath)
                .build();
        DataStreamSource<String> lines2 = env2.fromSource(
                source2,
                WatermarkStrategy.noWatermarks(),
                "GzipCsvSource"
        );
        
        // Second batch: process remaining records (records > MAX_BATCH_SIZE)
        DataStream<CovidPOJO> secondBatch = lines2.map(new MapFunction<String, CovidPOJO>() {
            private int recordCount = 0;
            
            @Override
            public CovidPOJO map(String value) throws Exception {
                recordCount++;
                // Only process records after MAX_BATCH_SIZE
                if (recordCount > MAX_BATCH_SIZE && recordCount <= EXPECTED_ROWS) {
                    return new CovidPOJO(value);
                }
                return null;
            }
        }).setParallelism(1).filter(record -> record != null);
        
        secondBatch.sinkTo(secondBatchSink);

        // The second batch should fail due to schema changes
        // We expect 0 additional rows to be inserted
        int secondBatchRows = executeAsyncJob(env2, tableName, 10, MAX_BATCH_SIZE);
        Assertions.assertEquals(MAX_BATCH_SIZE, secondBatchRows,
                "Expected 0 rows from second batch due to schema mismatch, but table have already : " + MAX_BATCH_SIZE);

        // Total should still be just the first batch
        int totalRows = ClickHouseServerForTests.countRows(tableName);
        Assertions.assertEquals(MAX_BATCH_SIZE, totalRows,
                "Expected total of " + MAX_BATCH_SIZE + " rows, but got: " + totalRows);
    }

    @Test
    void DataCorruptionCSVTest() throws Exception {
        String tableName = "csv_corruption_test";
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
        ClickHouseConvertor<String> convertorString = new ClickHouseConvertor<>(String.class);
        // create sink
        ClickHouseAsyncSink<String> csvSink = ClickHouseAsyncSink.<String>builder()
                .setElementConverter(convertorString)
                .setMaxBatchSize(MAX_BATCH_SIZE)
                .setMaxInFlightRequests(MAX_IN_FLIGHT_REQUESTS)
                .setMaxBufferedRequests(MAX_BUFFERED_REQUESTS)
                .setMaxBatchSizeInBytes(MAX_BATCH_SIZE_IN_BYTES)
                .setMaxTimeInBufferMS(MAX_TIME_IN_BUFFER_MS)
                .setMaxRecordSizeInBytes(MAX_RECORD_SIZE_IN_BYTES)
                .setClickHouseClientConfig(clickHouseClientConfig)
                .setClickHouseFormat(ClickHouseFormat.CSV)
                .build();

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
        
        // Create two distinct batches: first MAX_BATCH_SIZE good records, next MAX_BATCH_SIZE corrupted records
        DataStream<String> testData = lines.map(new MapFunction<String, String>() {
            private int recordCount = 0;
            
            @Override
            public String map(String value) throws Exception {
                recordCount++;
                // First MAX_BATCH_SIZE records: keep as-is (good records)
                // Next MAX_BATCH_SIZE records: corrupt the new_confirmed field
                if (recordCount > MAX_BATCH_SIZE) {
                    String[] parts = value.split(",");
                    if (parts.length >= 3) {
                        parts[2] = "invalid_number"; // corrupt new_confirmed field
                        return String.join(",", parts);
                    }
                }
                return value;
            }
        }).setParallelism(1);
        
        testData.sinkTo(csvSink);
        
        // We expect only the first MAX_BATCH_SIZE good records to be inserted
        // The corrupted batch should be rejected
        int rows = executeAsyncJob(env, tableName, 10, MAX_BATCH_SIZE);
        
        // Should have exactly MAX_BATCH_SIZE rows (the good batch)
        Assertions.assertEquals(MAX_BATCH_SIZE, rows,
            "Expected exactly " + MAX_BATCH_SIZE + " rows from good batch, but got: " + rows);
    }

    /**
     * Tests that POJO data survives checkpoint/restore via the rehydration path.
     *
     * A FailingSource emits all records, waits for a checkpoint to complete,
     * then throws an exception. After Flink restarts from checkpoint, the source
     * emits nothing. Any data that arrives in ClickHouse after the restart
     * must have come from checkpoint state — proving the originalInput was
     * persisted and re-serialized (rehydrated) correctly.
     */
    @Test
    void CheckpointRestoreWithRehydrationTest() throws Exception {
        if (isCloud()) return;

        String tableName = "checkpoint_rehydration_pojo";
        int expectedRows = 100;

        ClickHouseServerForTests.executeSql(SimplePOJO.createTableSQL(getDatabase(), tableName));

        TableSchema tableSchema = ClickHouseServerForTests.getTableSchema(tableName);
        POJOConvertor<SimplePOJO> simplePOJOConvertor = new SimplePOJOConvertor(tableSchema.hasDefaults());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // Enable checkpointing every 1 second
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);
        // Allow 1 restart after failure via configuration
        org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
        config.set(org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        config.set(org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, java.time.Duration.ofSeconds(1));
        env.configure(config);

        ClickHouseClientConfig clickHouseClientConfig = new ClickHouseClientConfig(
                getServerURL(), getUsername(), getPassword(), getDatabase(), tableName);
        clickHouseClientConfig.setSupportDefault(tableSchema.hasDefaults());

        ClickHouseConvertor<SimplePOJO> converter =
                new ClickHouseConvertor<>(SimplePOJO.class, simplePOJOConvertor);

        // Use a large buffer time so records stay buffered (not flushed) before checkpoint
        ClickHouseAsyncSink<SimplePOJO> sink = ClickHouseAsyncSink.<SimplePOJO>builder()
                .setElementConverter(converter)
                .setMaxBatchSize(MAX_BATCH_SIZE)
                .setMaxInFlightRequests(MAX_IN_FLIGHT_REQUESTS)
                .setMaxBufferedRequests(MAX_BUFFERED_REQUESTS)
                .setMaxBatchSizeInBytes(MAX_BATCH_SIZE_IN_BYTES)
                .setMaxTimeInBufferMS(10 * 1000) // 10 seconds buffer time
                .setMaxRecordSizeInBytes(MAX_RECORD_SIZE_IN_BYTES)
                .setClickHouseClientConfig(clickHouseClientConfig)
                .build();

        List<SimplePOJO> pojoList = new ArrayList<>();
        for (int i = 0; i < expectedRows; i++) {
            pojoList.add(new SimplePOJO(i));
        }

        final String checkTableName = tableName;
        FailingSource.RowCountChecker rowCountChecker = () -> ClickHouseServerForTests.countRows(checkTableName);
        DataStream<SimplePOJO> stream = env.addSource(new FailingSource<>(pojoList, rowCountChecker))
                .returns(SimplePOJO.class);
        stream.sinkTo(sink);

        // The job will: emit records → checkpoint → fail → restore → flush from state → data arrives
        int rows = executeAsyncJob(env, tableName, 60, expectedRows);
        Assertions.assertEquals(expectedRows, rows,
                "All records should arrive via checkpoint restore + rehydration");
    }

    /**
     * Tests that partial flush + checkpoint restore works correctly.
     *
     * Uses a small batch size to force the sink to flush some records to ClickHouse
     * before the checkpoint. After the failure and restore, the remaining records
     * (from checkpoint state) are rehydrated and flushed. The total must equal all records.
     *
     * Verifies: rows_before_checkpoint + rows_from_rehydration = total_records
     */
    @Test
    void CheckpointRestoreWithPartialFlushTest() throws Exception {
        if (isCloud()) return;

        String tableName = "checkpoint_partial_flush_pojo";
        int expectedRows = 200;
        int smallBatchSize = 10;

        ClickHouseServerForTests.executeSql(SimplePOJO.createTableSQL(getDatabase(), tableName));

        TableSchema tableSchema = ClickHouseServerForTests.getTableSchema(tableName);
        POJOConvertor<SimplePOJO> simplePOJOConvertor = new SimplePOJOConvertor(tableSchema.hasDefaults());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2000);
        env.getCheckpointConfig().setCheckpointingMode(org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);
        org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
        config.set(org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        config.set(org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, java.time.Duration.ofSeconds(1));
        env.configure(config);

        ClickHouseClientConfig clickHouseClientConfig = new ClickHouseClientConfig(
                getServerURL(), getUsername(), getPassword(), getDatabase(), tableName);
        clickHouseClientConfig.setSupportDefault(tableSchema.hasDefaults());

        ClickHouseConvertor<SimplePOJO> converter =
                new ClickHouseConvertor<>(SimplePOJO.class, simplePOJOConvertor);

        ClickHouseAsyncSink<SimplePOJO> sink = ClickHouseAsyncSink.<SimplePOJO>builder()
                .setElementConverter(converter)
                .setMaxBatchSize(smallBatchSize)
                .setMaxInFlightRequests(MAX_IN_FLIGHT_REQUESTS)
                .setMaxBufferedRequests(MAX_BUFFERED_REQUESTS)
                .setMaxBatchSizeInBytes(MAX_BATCH_SIZE_IN_BYTES)
                .setMaxTimeInBufferMS(1000)
                .setMaxRecordSizeInBytes(MAX_RECORD_SIZE_IN_BYTES)
                .setClickHouseClientConfig(clickHouseClientConfig)
                .build();

        List<SimplePOJO> pojoList = new ArrayList<>();
        for (int i = 0; i < expectedRows; i++) {
            pojoList.add(new SimplePOJO(i));
        }

        final String checkTableName = tableName;
        FailingSource.RowCountChecker rowCountChecker = () -> ClickHouseServerForTests.countRows(checkTableName);
        DataStream<SimplePOJO> stream = env.addSource(
                        new FailingSource<>(pojoList, rowCountChecker, false))
                .returns(SimplePOJO.class);
        stream.sinkTo(sink);

        int rows = executeAsyncJob(env, tableName, 60, expectedRows);
        Assertions.assertEquals(expectedRows, rows,
                "All records should arrive: some flushed before checkpoint + rest from rehydration");
    }
}
