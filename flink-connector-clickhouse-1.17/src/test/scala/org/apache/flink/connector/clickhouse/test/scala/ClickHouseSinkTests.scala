package org.apache.flink.connector.clickhouse.test.scala

import com.clickhouse.data.ClickHouseFormat
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor
import org.apache.flink.connector.clickhouse.sink.{ClickHouseAsyncSink, ClickHouseClientConfig}
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.TextLineInputFormat
import org.apache.flink.connector.test.FlinkClusterTests
import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests
import org.apache.flink.connector.test.embedded.flink.EmbeddedFlinkClusterForTests
import org.apache.flink.core.execution.JobClient
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ClickHouseSinkTests extends AnyFunSuite with BeforeAndAfterAll {

  val EXPECTED_ROWS = 10000

  override def beforeAll(): Unit = {
    FlinkClusterTests.setUp()
  }

  override def afterAll(): Unit = {
    FlinkClusterTests.tearDown()
  }

  @throws[Exception]
  private def executeJob(env: StreamExecutionEnvironment, tableName: String) = {
    val jobClient : JobClient = env.executeAsync("Read GZipped CSV with FileSource")
    var rows : Integer = 0
    var iterations : Integer = 0
    var continue : Boolean = true
    while (iterations < 10 && continue) {
      Thread.sleep(1000)
      iterations += 1
      rows = ClickHouseServerForTests.countRows(tableName)
      System.out.println("Rows: " + rows)
      if (rows == EXPECTED_ROWS) continue = false //todo: break is not supported
    }
    // cancel job
    jobClient.cancel
    rows
  }

  test("csv data") {
    val tableName = "csv_scala_covid"
    val dropTable = String.format("DROP TABLE IF EXISTS `%s`.`%s`", FlinkClusterTests.getDatabase, tableName)
    ClickHouseServerForTests.executeSql(dropTable)
    // create table
    val tableSql = "CREATE TABLE `" + FlinkClusterTests.getDatabase + "`.`" + tableName + "` (" +
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
      "ORDER BY (location_key, date); "

    ClickHouseServerForTests.executeSql(tableSql);
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1);

    val clickHouseClientConfig = new ClickHouseClientConfig(FlinkClusterTests.getServerURL, FlinkClusterTests.getUsername, FlinkClusterTests.getPassword, FlinkClusterTests.getDatabase, tableName);
    val convertorString: ClickHouseConvertor[String] = new ClickHouseConvertor(classOf[String]);
    // create sink
    val csvSink = new ClickHouseAsyncSink[String](
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
    csvSink.setClickHouseFormat(ClickHouseFormat.CSV);

    val filePath = new Path("./src/test/resources/epidemiology_top_10000.csv.gz");

    val source = FileSource
      .forRecordStreamFormat(new TextLineInputFormat(), filePath)
      .build();

    val lines = env.fromSource(
      source,
      WatermarkStrategy.noWatermarks(),
      "GzipCsvSource"
    );
    lines.sinkTo(csvSink);
    val rows = executeJob(env, tableName);
    assert(EXPECTED_ROWS == rows);
  }

}
