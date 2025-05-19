package org.apache.flink.connector.test.embedded.clickhouse;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.testcontainers.clickhouse.ClickHouseContainer;

public class ClickHouseServerForTests {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseServerForTests.class);

    protected static boolean isCloud = ClickHouseTestHelpers.isCloud();
    protected static String database = null;
    protected static ClickHouseContainer db = null;

    protected static String host = null;
    protected static int port = 0;
    protected static String username = null;
    protected static String password = null;
    protected static boolean isSSL = false;


    public static void initConfiguration() {
        if (isCloud) {
            host = System.getenv("CLICKHOUSE_CLOUD_HOST");
            port = Integer.parseInt(System.getenv("CLICKHOUSE_CLOUD_PORT"));
            database = System.getenv("CLICKHOUSE_DATABASE");
            username = System.getenv("CLICKHOUSE_USERNAME");
            password = System.getenv("CLICKHOUSE_PASSWORD");
        } else {
            host = db.getHost();
            port = db.getFirstMappedPort();
            database = ClickHouseTestHelpers.DATABASE_DEFAULT;
            username = db.getUsername();
            password = db.getPassword();
        }
        isSSL = ClickHouseTestHelpers.isCloud();
    }
    public static void setUp() {
        if (database == null) {
            database = String.format("flink_connector_test_%s", System.currentTimeMillis());
        }
        if (!isCloud) {
            db = new ClickHouseContainer(ClickHouseTestHelpers.CLICKHOUSE_DOCKER_IMAGE).withPassword("test_password").withEnv("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1");
            db.start();
        }
        initConfiguration();
        if (isCloud) {
            // wakeup cloud
            // have a for loop
            boolean isLive = false;
            int counter = 0;
            while (isLive || counter < 3) {
                isLive = ClickHouseTestHelpers.ping(isCloud, host, port, isSSL, username, password);
                counter++;
            }
            if (!isLive)
                throw new RuntimeException("Failed to connect to ClickHouse");
        }
    }

    public static void tearDown() {
        if (db != null) {
            db.stop();
        }
    }

    public static String getDataBase() { return database; }

    public static void executeSql(String sql) throws ExecutionException, InterruptedException {
        Client client = ClickHouseTestHelpers.getClient(isCloud, host, port, isSSL, username, password);
        client.execute(sql).get();
    }

    public static int countRows(String table) throws ExecutionException, InterruptedException {
        String countSql = String.format("SELECT COUNT(*) FROM `%s`.`%s`", database, table);
        Client client = ClickHouseTestHelpers.getClient(isCloud, host, port, isSSL, username, password);
        List<GenericRecord> countResult = client.queryAll(countSql);
        return countResult.get(0).getInteger(1);
    }
}
