package org.apache.flink.connector.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.apache.flink.connector.test.embedded.flink.EmbeddedFlinkClusterForTests;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

class DummyFlinkClusterTest extends FlinkClusterTests {
    // A simple Collection Sink
    private static class CollectSink implements SinkFunction<Long> {

        // must be static
        public static final List<Long> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(Long value, SinkFunction.Context context) throws Exception {
            values.add(value);
        }
    }

    public static class IncrementMapFunction implements MapFunction<Long, Long> {

        @Override
        public Long map(Long record) throws Exception {
            return record + 1;
        }
    }

    @Test
    void testDummyFlinkCluster() throws Exception {
        MiniClusterWithClientResource flinkCluster = EmbeddedFlinkClusterForTests.getMiniCluster();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        CollectSink.values.clear();

        env.fromElements(1L, 2L)
            .setParallelism(1)
            .map(new IncrementMapFunction())
            .addSink(new CollectSink());
        env.execute("testDummyFlinkCluster");
        Assertions.assertEquals(2, CollectSink.values.size());
    }

    @Test
    void testClickHouse() throws ExecutionException, InterruptedException {
        String tableName = "clickhouse_test";
        String createTableSQl = String.format("CREATE TABLE `%s`.`%s` (order_id UInt64) ENGINE = MergeTree ORDER BY tuple(order_id);", getDatabase(), tableName);
        ClickHouseServerForTests.executeSql(createTableSQl);
        int rows = ClickHouseServerForTests.countRows(tableName);
        Assertions.assertEquals(0, rows);
    }
}
