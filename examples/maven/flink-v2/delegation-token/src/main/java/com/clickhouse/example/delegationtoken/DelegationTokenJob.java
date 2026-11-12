package com.clickhouse.example.delegationtoken;

import com.clickhouse.data.ClickHouseFormat;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

/**
 * Writes rows to ClickHouse Cloud authenticated only by Flink delegation tokens: the job has no
 * username or password. Requires flink-connector-clickhouse-token in Flink's lib/ and
 * security.delegation.token.provider.clickhouse.oauth.* in the Flink configuration.
 *
 * <p>Rows are emitted slowly ({@code -intervalMs}) so the job outlives at least one token rotation.
 *
 * <pre>
 * CREATE TABLE &lt;database&gt;.&lt;table&gt; (id UInt64, payload String) ENGINE = MergeTree ORDER BY id
 * </pre>
 */
public class DelegationTokenJob {

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		final long rows = params.getLong("rows", 120);
		final long intervalMs = params.getLong("intervalMs", 1000);

		ClickHouseClientConfig clickHouseClientConfig = ClickHouseClientConfig.forDelegationToken(
				params.getRequired("url"), params.getRequired("database"), params.getRequired("table"));

		ClickHouseAsyncSink<String> sink = ClickHouseAsyncSink.<String>builder()
				.setElementConverter(new ClickHouseConvertor<>(String.class))
				.setMaxBatchSize(10)
				.setMaxInFlightRequests(2)
				.setMaxBufferedRequests(1000)
				.setMaxBatchSizeInBytes(1024 * 1024)
				.setMaxTimeInBufferMS(2000)
				.setMaxRecordSizeInBytes(1000)
				.setClickHouseClientConfig(clickHouseClientConfig)
				.setClickHouseFormat(ClickHouseFormat.CSV)
				.build();

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);
		env.fromSequence(1, rows)
				.map(id -> {
					Thread.sleep(intervalMs);
					return id + ",flink-dt-cluster-" + id;
				})
				.returns(Types.STRING)
				.sinkTo(sink);
		env.execute("ClickHouse sink with Flink delegation tokens");
	}
}
