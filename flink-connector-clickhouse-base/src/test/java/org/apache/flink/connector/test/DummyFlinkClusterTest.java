package org.apache.flink.connector.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class DummyFlinkClusterTest extends EmbeddedFlinkClusterForTests {

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
        TestStreamEnvironment env = flinkCluster.getTestStreamEnvironment();

        CollectSink.values.clear();

        env.fromData(1L, 2L)
            .map(new IncrementMapFunction())
            .addSink(new CollectSink());
        env.execute("testDummyFlinkCluster");
        Assertions.assertEquals(2, CollectSink.values.size());
        Assertions.assertEquals(2L, CollectSink.values.get(0));
        Assertions.assertEquals(3L, CollectSink.values.get(1));
    }
}
