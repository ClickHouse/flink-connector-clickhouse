package org.apache.flink.connector.clickhouse.table;

import com.clickhouse.client.api.query.GenericRecord;

import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

/** End-to-end Flink SQL round trips against a real ClickHouse, plus a planning-time rejection. */
public class ClickHouseTableApiIntegrationTests {

    @BeforeAll
    public static void setUp() throws Exception {
        ClickHouseServerForTests.setUp();
    }

    @AfterAll
    public static void tearDown() {
        ClickHouseServerForTests.tearDown();
    }

    private static TableEnvironment tableEnvironment() {
        return TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    }

    private static String sinkDdl(String flinkTable, String clickHouseTable, String columns) {
        return String.format(
                "CREATE TABLE %s (%s) WITH ("
                        + "'connector' = 'clickhouse',"
                        + "'url' = '%s',"
                        + "'username' = '%s',"
                        + "'password' = '%s',"
                        + "'database' = '%s',"
                        + "'table' = '%s')",
                flinkTable, columns,
                ClickHouseServerForTests.getURL(),
                ClickHouseServerForTests.getUsername(),
                ClickHouseServerForTests.getPassword(),
                ClickHouseServerForTests.getDatabase(),
                clickHouseTable);
    }

    @Test
    void sqlInsertRoundTripsThroughClickHouse() throws Exception {
        String table = "table_api_events";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` ("
                        + "id Int64, name String, amount Decimal(18, 4), created_at DateTime64(3), "
                        + "uid UUID, event_day Date, is_active Bool, score Float64, "
                        + "tags Array(String), props Map(String, String)"
                        + ") ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_events", table,
                "id BIGINT NOT NULL,"
                        + "name STRING NOT NULL,"
                        + "amount DECIMAL(18, 4) NOT NULL,"
                        + "created_at TIMESTAMP(3) NOT NULL,"
                        + "uid STRING NOT NULL,"
                        + "event_day DATE NOT NULL,"
                        + "is_active BOOLEAN NOT NULL,"
                        + "score DOUBLE NOT NULL,"
                        + "tags ARRAY<STRING NOT NULL> NOT NULL,"
                        + "props MAP<STRING, STRING NOT NULL> NOT NULL"));

        String insert = "INSERT INTO ch_events VALUES "
                + "(1, 'alice', CAST(12.5 AS DECIMAL(18, 4)), TIMESTAMP '2026-01-02 03:04:05.678', "
                + "'f47ac10b-58cc-4372-a567-0e02b2c3d479', DATE '2026-01-02', true, 99.25, "
                + "ARRAY['a', 'b'], MAP['k1', 'v1']), "
                + "(2, 'bob', CAST(7 AS DECIMAL(18, 4)), TIMESTAMP '2026-01-02 03:04:06', "
                + "'123e4567-e89b-12d3-a456-426614174000', DATE '1970-01-01', false, -1.5, "
                + "ARRAY['x'], MAP['k2', 'v2'])";

        // EXPLAIN re-invokes the factory; the INSERT below plans again and re-introspects.
        Assertions.assertFalse(env.explainSql(insert).isEmpty());
        env.executeSql(insert).await();

        List<GenericRecord> rows = ClickHouseServerForTests.extractData(
                "id, name, amount, toString(created_at) AS created_at_s, toString(uid) AS uid_s, "
                        + "toString(event_day) AS day_s, is_active, score, toString(tags) AS tags_s, "
                        + "toString(props) AS props_s",
                ClickHouseServerForTests.getDatabase(), table, "id");

        Assertions.assertEquals(2, rows.size());
        GenericRecord first = rows.get(0);
        Assertions.assertEquals(1L, first.getLong("id"));
        Assertions.assertEquals("alice", first.getString("name"));
        Assertions.assertEquals(0, new BigDecimal("12.5").compareTo(first.getBigDecimal("amount")));
        Assertions.assertEquals("2026-01-02 03:04:05.678", first.getString("created_at_s"));
        Assertions.assertEquals("f47ac10b-58cc-4372-a567-0e02b2c3d479", first.getString("uid_s"));
        Assertions.assertEquals("2026-01-02", first.getString("day_s"));
        Assertions.assertTrue(first.getBoolean("is_active"));
        Assertions.assertEquals(99.25, first.getDouble("score"));
        Assertions.assertEquals("['a','b']", first.getString("tags_s"));
        Assertions.assertEquals("{'k1':'v1'}", first.getString("props_s"));

        GenericRecord second = rows.get(1);
        Assertions.assertEquals(2L, second.getLong("id"));
        Assertions.assertEquals("2026-01-02 03:04:06.000", second.getString("created_at_s"));
        Assertions.assertEquals("1970-01-01", second.getString("day_s"));
        Assertions.assertFalse(second.getBoolean("is_active"));
        Assertions.assertEquals("['x']", second.getString("tags_s"));
    }

    @Test
    void replanningAfterAlterSeesTheCurrentSchema() throws Exception {
        String table = "table_api_alter";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64) ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_alter_before", table, "id BIGINT NOT NULL"));
        // Introspect once pre-ALTER, so a schema memo (were one to exist) would be populated.
        Assertions.assertFalse(env.explainSql("INSERT INTO ch_alter_before VALUES (1)").isEmpty());

        ClickHouseServerForTests.executeSql(String.format(
                "ALTER TABLE `%s`.`%s` ADD COLUMN label String",
                ClickHouseServerForTests.getDatabase(), table));

        // Same-JVM re-planning must see the post-ALTER schema and accept the new column.
        env.executeSql(sinkDdl("ch_alter_after", table, "id BIGINT NOT NULL, label STRING NOT NULL"));
        env.executeSql("INSERT INTO ch_alter_after VALUES (7, 'post-alter')").await();

        List<GenericRecord> rows = ClickHouseServerForTests.extractData(
                "id, label", ClickHouseServerForTests.getDatabase(), table, "id");
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(7L, rows.get(0).getLong("id"));
        Assertions.assertEquals("post-alter", rows.get(0).getString("label"));
    }

    @Test
    void unknownFlinkColumnFailsAtPlanningWithPreciseMessage() throws Exception {
        String table = "table_api_reject";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64) ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_reject", table,
                "id BIGINT NOT NULL, nickname STRING NOT NULL"));

        Exception e = Assertions.assertThrows(Exception.class,
                () -> env.executeSql("INSERT INTO ch_reject VALUES (1, 'nick')"));
        Assertions.assertTrue(exceptionChainContains(e,
                        "Column 'nickname' declared in the Flink schema does not exist in"),
                "Unexpected failure: " + e);
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
