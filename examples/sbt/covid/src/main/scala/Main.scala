import com.clickhouse.data.ClickHouseFormat
import org.apache.flink.connector.clickhouse.sink.{ClickHouseAsyncSink, ClickHouseClientConfig}
import org.apache.flink.util.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.base.sink.writer.ElementConverter
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor
import org.apache.flink.connector.clickhouse.data.ClickHousePayload
import org.apache.flink.connector.file.src.reader.TextLineInputFormat
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object Main extends App {

  private val MAX_BATCH_SIZE = 5000
  private val MAX_IN_FLIGHT_REQUESTS = 2
  private val MAX_BUFFERED_REQUESTS = 20000
  private val MAX_BATCH_SIZE_IN_BYTES = 1024 * 1024L
  private val MAX_TIME_IN_BUFFER_MS = 5 * 1000L
  private val MAX_RECORD_SIZE_IN_BYTES = 1000L

  /*
		Create covid table before running the example
		CREATE TABLE `default`.`covid` (
							date Date,
                            location_key LowCardinality(String),
                            new_confirmed Int32,
                            new_deceased Int32,
                            new_recovered Int32,
                            new_tested Int32,
                            cumulative_confirmed Int32,
                            cumulative_deceased Int32,
                            cumulative_recovered Int32,
                            cumulative_tested Int32
                            )
                            ENGINE = MergeTree
                            ORDER BY (location_key, date);
	 */

  val parameters: ParameterTool = ParameterTool.fromArgs(args)
  val fileFullName = parameters.get("input")
  val url = parameters.get("url")
  val username = parameters.get("username")
  val password = parameters.get("password")
  val database = parameters.get("database")
  val tableName = parameters.get("table")

  val clickHouseClientConfig : ClickHouseClientConfig = new ClickHouseClientConfig(url, username, password, database, tableName)

  val convertorString: ElementConverter[String, ClickHousePayload] =
    new ClickHouseConvertor[String](classOf[String])

  val csvSink: ClickHouseAsyncSink[String] = new ClickHouseAsyncSink[String](
    convertorString,
    MAX_BATCH_SIZE,
    MAX_IN_FLIGHT_REQUESTS,
    MAX_BUFFERED_REQUESTS,
    MAX_BATCH_SIZE_IN_BYTES,
    MAX_TIME_IN_BUFFER_MS,
    MAX_RECORD_SIZE_IN_BYTES,
    clickHouseClientConfig
  )
  csvSink.setClickHouseFormat(ClickHouseFormat.CSV)

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(2)

  val filePath = new Path(fileFullName)
  val source: FileSource[String] = FileSource
    .forRecordStreamFormat(new TextLineInputFormat(), filePath)
    .build()

  val lines: DataStreamSource[String] = env.fromSource(
    source,
    WatermarkStrategy.noWatermarks[String](),
    "GzipCsvSource"
  )

  lines.sinkTo(csvSink)
  env.execute("Flink Scala API Read CSV (covid)")
}