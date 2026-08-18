package org.apache.flink.connector.clickhouse.table.catalog;

import com.clickhouse.client.api.query.GenericRecord;

import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

/** Minimal end-to-end check of the read-only catalog against a real ClickHouse. */
public class ClickHouseCatalogIntegrationTests {

    @BeforeAll
    public static void setUp() throws Exception {
        ClickHouseServerForTests.setUp();
    }

    @AfterAll
    public static void tearDown() {
        ClickHouseServerForTests.tearDown();
    }

    private static TableEnvironment environmentWithCatalog() {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.executeSql(String.format(
                "CREATE CATALOG ch WITH ("
                        + "'type' = 'clickhouse',"
                        + "'url' = '%s',"
                        + "'username' = '%s',"
                        + "'password' = '%s',"
                        + "'default-database' = '%s')",
                ClickHouseServerForTests.getURL(),
                ClickHouseServerForTests.getUsername(),
                ClickHouseServerForTests.getPassword(),
                ClickHouseServerForTests.getDatabase()));
        return env;
    }

    /** No per-table DDL: list the real table, insert through it, read the rows back. */
    @Test
    void catalogExposesRealTablesAndInsertRoundTrips() throws Exception {
        String database = ClickHouseServerForTests.getDatabase();
        String table = "catalog_events";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` ("
                        + "id Int64, name LowCardinality(String), qty UInt32, price Nullable(Float64), "
                        + "created_at DateTime64(3, 'UTC'), tags Array(String)"
                        + ") ENGINE = MergeTree() ORDER BY id",
                database, table));

        TableEnvironment env = environmentWithCatalog();

        Catalog catalog = env.getCatalog("ch").orElseThrow(AssertionError::new);
        Assertions.assertTrue(catalog.databaseExists(database));
        Assertions.assertTrue(catalog.listTables(database).contains(table));

        env.executeSql(String.format(
                "INSERT INTO ch.`%s`.`%s` VALUES "
                        + "(1, 'alice', 7, CAST(1.5 AS DOUBLE), TO_TIMESTAMP_LTZ(1735786800123, 3), ARRAY['a', 'b']), "
                        + "(2, 'bob', 8, CAST(NULL AS DOUBLE), TO_TIMESTAMP_LTZ(1735786801000, 3), ARRAY['x'])",
                database, table)).await();

        List<GenericRecord> rows = ClickHouseServerForTests.extractData(
                "id, name, qty, coalesce(toString(price), 'null') AS price_s, "
                        + "toString(created_at) AS created_at_s, toString(tags) AS tags_s",
                database, table, "id");

        Assertions.assertEquals(2, rows.size());
        GenericRecord first = rows.get(0);
        Assertions.assertEquals(1L, first.getLong("id"));
        Assertions.assertEquals("alice", first.getString("name"));
        Assertions.assertEquals(7L, first.getLong("qty"));
        Assertions.assertEquals("1.5", first.getString("price_s"));
        Assertions.assertEquals("2025-01-02 03:00:00.123", first.getString("created_at_s"));
        Assertions.assertEquals("['a','b']", first.getString("tags_s"));

        GenericRecord second = rows.get(1);
        Assertions.assertEquals(2L, second.getLong("id"));
        Assertions.assertEquals("null", second.getString("price_s"));
        Assertions.assertEquals("2025-01-02 03:00:01.000", second.getString("created_at_s"));
        Assertions.assertEquals("['x']", second.getString("tags_s"));
    }

    @Test
    void mutatingDdlFailsAsReadOnly() {
        TableEnvironment env = environmentWithCatalog();
        Exception e = Assertions.assertThrows(Exception.class, () -> env.executeSql(String.format(
                "CREATE TABLE ch.`%s`.`catalog_readonly` (id BIGINT)",
                ClickHouseServerForTests.getDatabase())));
        Assertions.assertTrue(exceptionChainContains(e, "read-only"), "Unexpected failure: " + e);
    }

    private static boolean exceptionChainContains(Throwable t, String needle) {
        for (Throwable cause = t; cause != null; cause = cause.getCause()) {
            if (cause.getMessage() != null && cause.getMessage().contains(needle)) {
                return true;
            }
        }
        return false;
    }
}
