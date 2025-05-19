package org.apache.flink.connector.test;

import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.apache.flink.connector.test.embedded.flink.EmbeddedFlinkClusterForTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class FlinkClusterTests {
    @BeforeAll
    static void setup() throws Exception {
        EmbeddedFlinkClusterForTests.setUp();
        ClickHouseServerForTests.setUp();
    }

    @AfterAll
    static void tearDown() {
        EmbeddedFlinkClusterForTests.tearDown();
        ClickHouseServerForTests.tearDown();
    }

}
