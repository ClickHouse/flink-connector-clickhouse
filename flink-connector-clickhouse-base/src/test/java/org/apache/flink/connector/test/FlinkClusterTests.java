package org.apache.flink.connector.test;

import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.apache.flink.connector.test.embedded.flink.EmbeddedFlinkClusterForTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class FlinkClusterTests {

    @BeforeAll
    public static void setUp() throws Exception {
        EmbeddedFlinkClusterForTests.setUp();
        ClickHouseServerForTests.setUp();
    }

    @AfterAll
    public static void tearDown() {
        EmbeddedFlinkClusterForTests.tearDown();
        ClickHouseServerForTests.tearDown();
    }

    public static String getServerURL() {
        return ClickHouseServerForTests.getURL();
    }

    public static String getUsername() {
        return ClickHouseServerForTests.getUsername();
    }

    public static String getPassword() {
        return ClickHouseServerForTests.getPassword();
    }

    public static String getDatabase() {
        return ClickHouseServerForTests.getDatabase();
    }
}
