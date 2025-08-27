/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clickhouse.example.covid;

import com.clickhouse.data.ClickHouseFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	static final int MAX_BATCH_SIZE = 5000;
	static final int MAX_IN_FLIGHT_REQUESTS = 2;
	static final int MAX_BUFFERED_REQUESTS = 20000;
	static final long MAX_BATCH_SIZE_IN_BYTES = 1024 * 1024;
	static final long MAX_TIME_IN_BUFFER_MS = 5 * 1000;
	static final long MAX_RECORD_SIZE_IN_BYTES = 1000;

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

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		ParameterTool parameters = ParameterTool.fromArgs(args);
		final String fileFullName = parameters.get("input");
		final String url = parameters.get("url");
		final String username = parameters.get("username");
		final String password = parameters.get("password");
		final String database = parameters.get("database");
		final String tableName = parameters.get("table");

		ClickHouseClientConfig clickHouseClientConfig = new ClickHouseClientConfig(url, username, password, database, tableName);
		ElementConverter<String, ClickHousePayload> convertorString = new ClickHouseConvertor<>(String.class);

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


		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		Path filePath = new Path(fileFullName);
		FileSource<String> source = FileSource
				.forRecordStreamFormat(new TextLineInputFormat(), filePath)
				.build();

		DataStreamSource<String> lines = env.fromSource(
				source,
				WatermarkStrategy.noWatermarks(),
				"GzipCsvSource"
		);
		lines.sinkTo(csvSink);
		env.execute("Flink Java API Read CSV (covid)");
	}
}
