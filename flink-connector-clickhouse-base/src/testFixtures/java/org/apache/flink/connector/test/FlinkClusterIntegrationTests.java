package org.apache.flink.connector.test;

import com.clickhouse.flink.Cluster;
import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class FlinkClusterIntegrationTests {
    static Cluster cluster;
    @BeforeAll
    public static void setUp() throws Exception {
        String dataFile = "100k_epidemiology.csv";
        Cluster.Builder builder = new Cluster.Builder()
                .withTaskManagers(3)
                .withDataFile("/Users/mzitnik/clickhouse/dev/integrations/flink-connector-clickhouse/flink-connector-clickhouse-base/src/test/resources/data/", dataFile, "/tmp")
                .withFlinkVersion("latest");
        cluster = builder.build();
        ClickHouseServerForTests.setUp();
    }

    @AfterAll
    public static void tearDown() {
        cluster.tearDown();
        ClickHouseServerForTests.tearDown();
    }

    public static String getServerURL() {
        return ClickHouseServerForTests.getURL();
    }

    public static String getIncorrectServerURL() {
        return ClickHouseServerForTests.getURL(ClickHouseServerForTests.getHost(), ClickHouseServerForTests.getPort() + 1);
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
