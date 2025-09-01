package org.apache.flink.connector.test.embedded.clickhouse;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.clickhouse.ClickHouseContainer;

import java.util.List;
import java.util.concurrent.ExecutionException;

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
            LOG.info("Init ClickHouse Cloud Configuration");
            host = System.getenv("CLICKHOUSE_CLOUD_HOST");
            port = Integer.parseInt(ClickHouseTestHelpers.HTTPS_PORT);
            database = String.format("flink_connector_test_%s", System.currentTimeMillis());
            username = ClickHouseTestHelpers.USERNAME_DEFAULT;
            password = System.getenv("CLICKHOUSE_CLOUD_PASSWORD");
        } else {
            LOG.info("Init ClickHouse Docker Configuration");
            host = db.getHost();
            port = db.getFirstMappedPort();
            database = ClickHouseTestHelpers.DATABASE_DEFAULT;
            username = db.getUsername();
            password = db.getPassword();
        }
        isSSL = ClickHouseTestHelpers.isCloud();
    }
    public static void setUp() throws InterruptedException, ExecutionException {
        if (!isCloud) {
            db = new ClickHouseContainer(ClickHouseTestHelpers.CLICKHOUSE_DOCKER_IMAGE).withPassword("test_password").withEnv("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1");
            db.start();
        }
        initConfiguration();
        // wakeup cloud
        // have a for loop
        boolean isLive = false;
        int counter = 0;
        while (counter < 5) {
            isLive = ClickHouseTestHelpers.ping(host, port, isSSL, username, password);
            if (isLive) {
                String createDatabase = String.format("CREATE DATABASE IF NOT EXISTS `%s`", database);
                executeSql(createDatabase);
                return;
            }
            Thread.sleep(2000);
            counter++;
        }
        throw new RuntimeException("Failed to connect to ClickHouse");
    }

    public static void tearDown() {
        if (db != null) {
            db.stop();
        }
    }

    public static String getDatabase() { return database; }

    public static String getHost() { return host; }
    public static int getPort() { return port; }
    public static String getUsername() { return username; }
    public static String getPassword() { return password; }

    public static String getURL() {
        return ClickHouseServerForTests.getURL(host, port);
    }

    public static String getURL(String host, int port) {
        if (isCloud) {
            return "https://" + host + ":" + port + "/";
        } else {
            return "http://" + host + ":" + port + "/";
        }
    }

    public static boolean isCloud() { return isCloud; }

    public static void executeSql(String sql) throws ExecutionException, InterruptedException {
        Client client = ClickHouseTestHelpers.getClient(host, port, isSSL, username, password);
        try {
            client.execute(sql).get().close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static int countParts(String table) {
        String countPartsSql = String.format("SELECT count(*) FROM system.parts WHERE table = '%s' and active = 1", table);
        Client client = ClickHouseTestHelpers.getClient(host, port, isSSL, username, password);
        List<GenericRecord> countResult = client.queryAll(countPartsSql);
        return countResult.get(0).getInteger(1);
    }

    public static int countMerges(String table) {
        String countPartsSql = String.format("SELECT count(*) FROM system.merges WHERE table = '%s'", table);
        Client client = ClickHouseTestHelpers.getClient(host, port, isSSL, username, password);
        List<GenericRecord> countResult = client.queryAll(countPartsSql);
        return countResult.get(0).getInteger(1);
    }

    public static int countRows(String table) throws ExecutionException, InterruptedException {
        String countSql = String.format("SELECT COUNT(*) FROM `%s`.`%s`", database, table);
        Client client = ClickHouseTestHelpers.getClient(host, port, isSSL, username, password);
        List<GenericRecord> countResult = client.queryAll(countSql);
        return countResult.get(0).getInteger(1);
    }

    // http_user_agent
    public static String extractProductName(String databaseName, String tableName, String startWith) {
        String extractProductName = String.format("SELECT http_user_agent, tables FROM clusterAllReplicas('default', system.query_log) WHERE type = 'QueryStart' AND query_kind = 'Insert' AND has(databases,'%s') AND has(tables,'%s.%s') and startsWith(http_user_agent, '%s') LIMIT 100", databaseName, databaseName, tableName, startWith);
        Client client = ClickHouseTestHelpers.getClient(host, port, isSSL, username, password);
        List<GenericRecord> userAgentResult = client.queryAll(extractProductName);
        String userAgentValue = null;
        if (!userAgentResult.isEmpty()) {
            for (GenericRecord userAgent : userAgentResult) {
                userAgentValue = userAgent.getString(1);
                if (userAgentValue.contains(startWith))
                    return userAgent.getString(1);
            }
            throw new RuntimeException("Can not extract product name from " + userAgentValue);
        }
        throw new RuntimeException("Query is returning empty result.");
    }

    public static TableSchema getTableSchema(String table) throws ExecutionException, InterruptedException {
        Client client = ClickHouseTestHelpers.getClient(host, port, isSSL, username, password);
        return client.getTableSchema(table, database);
    }
}
