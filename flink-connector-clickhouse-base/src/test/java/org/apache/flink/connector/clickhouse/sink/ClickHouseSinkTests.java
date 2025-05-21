package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.data.ClickHouseFormat;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.connector.test.FlinkClusterTests;
import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.apache.flink.connector.test.embedded.flink.EmbeddedFlinkClusterForTests;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;

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
        //String path = ClickHouseSinkTests.class.getClassLoader().getResource(".").toString();
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
        // in case of just want to forward our data use the appropriate ClickHouse format
        csvSink.setClickHouseFormat(ClickHouseFormat.TSV);

        Path filePath = new Path("./src/test/resources/epidemiology_top_10000.csv.gz");

        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), filePath)
                .build();

        DataStreamSource<String> lines = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "GzipCsvSource"
        );
        lines.sinkTo(csvSink);
        int rows = executeJob(env, tableName);
        Assertions.assertEquals(EXPECTED_ROWS, rows);
    }
}
