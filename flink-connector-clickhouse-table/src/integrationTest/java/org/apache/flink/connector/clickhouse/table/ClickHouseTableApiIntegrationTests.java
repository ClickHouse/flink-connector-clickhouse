package org.apache.flink.connector.clickhouse.table;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;

import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
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
        return sinkDdl(flinkTable, clickHouseTable, columns, "");
    }

    private static String sinkDdl(String flinkTable, String clickHouseTable, String columns,
                                  String extraOptions) {
        return String.format(
                "CREATE TABLE %s (%s) WITH ("
                        + "'connector' = 'clickhouse',"
                        + "'url' = '%s',"
                        + "'username' = '%s',"
                        + "'password' = '%s',"
                        + "'database' = '%s',"
                        + "'table' = '%s'%s)",
                flinkTable, columns,
                ClickHouseServerForTests.getURL(),
                ClickHouseServerForTests.getUsername(),
                ClickHouseServerForTests.getPassword(),
                ClickHouseServerForTests.getDatabase(),
                clickHouseTable, extraOptions);
    }

    @Test
    void sqlInsertRoundTripsThroughClickHouse() throws Exception {
        String table = "table_api_events";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` ("
                        + "id Int64, name String, amount Decimal(18, 4), created_at DateTime64(3), "
                        + "uid UUID, event_day Date, is_active Bool, score Float64, "
                        + "tags Array(String), props Map(String, String), "
                        + "category LowCardinality(String), code FixedString(4)"
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
                        + "props MAP<STRING, STRING NOT NULL> NOT NULL,"
                        + "category STRING NOT NULL,"
                        + "code STRING NOT NULL"));

        String insert = "INSERT INTO ch_events VALUES "
                + "(1, 'alice', CAST(12.5 AS DECIMAL(18, 4)), TIMESTAMP '2026-01-02 03:04:05.678', "
                + "'f47ac10b-58cc-4372-a567-0e02b2c3d479', DATE '2026-01-02', true, 99.25, "
                + "ARRAY['a', 'b'], MAP['k1', 'v1'], 'gold', 'AB12'), "
                + "(2, 'bob', CAST(7 AS DECIMAL(18, 4)), TIMESTAMP '2026-01-02 03:04:06', "
                + "'123e4567-e89b-12d3-a456-426614174000', DATE '1970-01-01', false, -1.5, "
                + "ARRAY['x'], MAP['k2', 'v2'], 'silver', 'ZZ99')";

        // EXPLAIN re-invokes the factory; the INSERT below plans again and re-introspects.
        Assertions.assertFalse(env.explainSql(insert).isEmpty());
        env.executeSql(insert).await();

        List<GenericRecord> rows = readBack(
                "id, name, amount, toString(created_at) AS created_at_s, toString(uid) AS uid_s, "
                        + "toString(event_day) AS day_s, is_active, score, toString(tags) AS tags_s, "
                        + "toString(props) AS props_s, category, toString(code) AS code_s",
                table, "id", 2);

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
        Assertions.assertEquals("gold", first.getString("category"));
        Assertions.assertEquals("AB12", first.getString("code_s"));

        GenericRecord second = rows.get(1);
        Assertions.assertEquals(2L, second.getLong("id"));
        Assertions.assertEquals("2026-01-02 03:04:06.000", second.getString("created_at_s"));
        Assertions.assertEquals("1970-01-01", second.getString("day_s"));
        Assertions.assertFalse(second.getBoolean("is_active"));
        Assertions.assertEquals("['x']", second.getString("tags_s"));
        Assertions.assertEquals("silver", second.getString("category"));
        Assertions.assertEquals("ZZ99", second.getString("code_s"));
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

        List<GenericRecord> rows = readBack("id, label", table, "id", 1);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(7L, rows.get(0).getLong("id"));
        Assertions.assertEquals("post-alter", rows.get(0).getString("label"));
    }

    @Test
    void planningPingIsNotGovernedBySinkMaxRetries() {
        TableEnvironment env = tableEnvironment();
        // Port 1 never listens, so every ping fails immediately with connection refused.
        env.executeSql(
                "CREATE TABLE ch_unreachable (id BIGINT NOT NULL) WITH ("
                        + "'connector' = 'clickhouse',"
                        + "'url' = 'http://localhost:1',"
                        + "'username' = 'default',"
                        + "'password' = '',"
                        + "'database' = 'default',"
                        + "'table' = 'whatever',"
                        + "'sink.max-retries' = '100000')");

        long start = System.nanoTime();
        Exception e = Assertions.assertThrows(Exception.class,
                () -> env.executeSql("INSERT INTO ch_unreachable VALUES (1)"));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        Assertions.assertTrue(exceptionChainContains(e, "not accessible"), "Unexpected failure: " + e);
        // The ping is a fixed 3 attempts with 1s pauses; 100 000 attempts would block for days.
        Assertions.assertTrue(elapsedMs < 60_000,
                "Planning ping blocked for " + elapsedMs + "ms — is sink.max-retries driving it again?");
    }

    @Test
    void batchRowsNotBelowBufferedRequestsIsRejectedAtPlanning() {
        TableEnvironment env = tableEnvironment();
        // Equal to the sink.max-buffered-requests default; the AsyncSink needs strictly greater.
        env.executeSql(sinkDdl("ch_invalid_buffering", "does_not_exist", "id BIGINT NOT NULL",
                ", 'sink.buffer-flush.max-rows' = '10000'"));

        Exception e = Assertions.assertThrows(Exception.class,
                () -> env.executeSql("INSERT INTO ch_invalid_buffering VALUES (1)"));
        Assertions.assertTrue(exceptionChainContains(e,
                        "'sink.max-buffered-requests' (10000) must be strictly greater than "
                                + "'sink.buffer-flush.max-rows' (10000)"),
                "Unexpected failure: " + e);
    }

    @Test
    void batchBytesBelowRecordBytesIsRejectedAtPlanning() {
        TableEnvironment env = tableEnvironment();
        // Below the 1mb sink.record.max-bytes default, so one record could never fit a batch.
        env.executeSql(sinkDdl("ch_invalid_bytes", "does_not_exist", "id BIGINT NOT NULL",
                ", 'sink.buffer-flush.max-bytes' = '512kb'"));

        Exception e = Assertions.assertThrows(Exception.class,
                () -> env.executeSql("INSERT INTO ch_invalid_bytes VALUES (1)"));
        Assertions.assertTrue(exceptionChainContains(e, "must be at least 'sink.record.max-bytes'"),
                "Unexpected failure: " + e);
    }

    @Test
    void negativeBigIntIntoUInt32FailsNamingTheColumn() throws Exception {
        String table = "table_api_unsigned";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64, hits UInt32) ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_unsigned", table, "id BIGINT NOT NULL, hits BIGINT NOT NULL"));

        Exception e = Assertions.assertThrows(Exception.class,
                () -> env.executeSql("INSERT INTO ch_unsigned VALUES (1, -1)").await());
        Assertions.assertTrue(exceptionChainContains(e,
                        "Column 'hits': value -1 is outside the UInt32 range"),
                "Unexpected failure: " + e);
    }

    @Test
    void outOfRangeDate32FailsNamingTheColumn() throws Exception {
        String table = "table_api_date32";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64, event_day Date32) ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_date32", table, "id BIGINT NOT NULL, event_day DATE NOT NULL"));

        // Pre-fix this was written raw and stored as a different date, with no error anywhere.
        Exception e = Assertions.assertThrows(Exception.class,
                () -> env.executeSql("INSERT INTO ch_date32 VALUES (1, DATE '9999-12-31')").await());
        Assertions.assertTrue(exceptionChainContains(e,
                        "Column 'event_day': DATE value 9999-12-31 is outside the ClickHouse Date32 range"),
                "Unexpected failure: " + e);
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

    @Test
    void nullValuesRoundTripIntoNullableColumns() throws Exception {
        String table = "table_api_nullable";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` ("
                        + "id Int64, name Nullable(String), score Nullable(Float64), event_day Nullable(Date)"
                        + ") ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_nullable", table,
                "id BIGINT NOT NULL, name STRING, score DOUBLE, event_day DATE"));
        env.executeSql("INSERT INTO ch_nullable VALUES "
                + "(1, 'alice', 99.5, DATE '2026-01-02'), "
                + "(2, CAST(NULL AS STRING), CAST(NULL AS DOUBLE), CAST(NULL AS DATE))").await();

        List<GenericRecord> rows = readBack(
                "id, ifNull(name, '<null>') AS name_s, ifNull(toString(score), '<null>') AS score_s, "
                        + "ifNull(toString(event_day), '<null>') AS day_s",
                table, "id", 2);

        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals("alice", rows.get(0).getString("name_s"));
        Assertions.assertEquals("99.5", rows.get(0).getString("score_s"));
        Assertions.assertEquals("2026-01-02", rows.get(0).getString("day_s"));
        Assertions.assertEquals("<null>", rows.get(1).getString("name_s"));
        Assertions.assertEquals("<null>", rows.get(1).getString("score_s"));
        Assertions.assertEquals("<null>", rows.get(1).getString("day_s"));
    }

    @Test
    void multisetRoundTripsIntoUInt64CountMap() throws Exception {
        String table = "table_api_multiset";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64, tags Map(String, UInt64)) "
                        + "ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        // Flink SQL has no MULTISET literal; COLLECT in batch mode emits final, insert-only rows.
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        env.executeSql(sinkDdl("ch_multiset", table,
                "id BIGINT NOT NULL, tags MULTISET<STRING NOT NULL> NOT NULL"));
        env.executeSql("INSERT INTO ch_multiset "
                + "SELECT id, COLLECT(tag) FROM ("
                + "  SELECT CAST(id AS BIGINT) AS id, CAST(tag AS STRING) AS tag "
                + "  FROM (VALUES (1, 'a'), (1, 'a'), (1, 'b'), (2, 'z')) AS t(id, tag)"
                + ") GROUP BY id").await();

        // Map entry order is not deterministic, so probe by key instead of comparing strings.
        List<GenericRecord> rows = readBack(
                "id, toInt64(tags['a']) AS a_cnt, toInt64(tags['b']) AS b_cnt, "
                        + "toInt64(tags['z']) AS z_cnt, toInt64(length(tags)) AS n_keys",
                table, "id", 2);

        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals(2L, rows.get(0).getLong("a_cnt"));
        Assertions.assertEquals(1L, rows.get(0).getLong("b_cnt"));
        Assertions.assertEquals(2L, rows.get(0).getLong("n_keys"));
        Assertions.assertEquals(1L, rows.get(1).getLong("z_cnt"));
        Assertions.assertEquals(1L, rows.get(1).getLong("n_keys"));
    }

    @Test
    void rowsWriteIntoTuplesAtEveryNestingAndNullArrayElementsRoundTrip() throws Exception {
        String table = "table_api_tuple";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` ("
                        + "id Int64, pair Tuple(Int32, String), nums Array(Nullable(Int32)), "
                        + "pairs Array(Tuple(Int32, String)), nested Tuple(Int32, Tuple(Int32, String))"
                        + ") ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_tuple", table,
                "id BIGINT NOT NULL,"
                        + "pair ROW<a INT NOT NULL, b STRING NOT NULL> NOT NULL,"
                        + "nums ARRAY<INT> NOT NULL,"
                        + "pairs ARRAY<ROW<a INT NOT NULL, b STRING NOT NULL> NOT NULL> NOT NULL,"
                        + "nested ROW<a INT NOT NULL, b ROW<c INT NOT NULL, d STRING NOT NULL> NOT NULL> NOT NULL"));
        env.executeSql("INSERT INTO ch_tuple VALUES "
                + "(1, ROW(7, 'x'), ARRAY[1, CAST(NULL AS INT), 3], "
                + "ARRAY[ROW(1, 'p'), ROW(2, 'q')], ROW(5, ROW(6, 'z')))").await();

        List<GenericRecord> rows = readBack(
                "id, toString(pair) AS pair_s, toString(nums) AS nums_s, "
                        + "toString(pairs) AS pairs_s, toString(nested) AS nested_s",
                table, "id", 1);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("(7,'x')", rows.get(0).getString("pair_s"));
        Assertions.assertEquals("[1,NULL,3]", rows.get(0).getString("nums_s"));
        Assertions.assertEquals("[(1,'p'),(2,'q')]", rows.get(0).getString("pairs_s"));
        Assertions.assertEquals("(5,(6,'z'))", rows.get(0).getString("nested_s"));
    }

    @Test
    void simpleAggregateFunctionColumnAcceptsItsInnerType() throws Exception {
        String table = "table_api_simple_agg";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64, total SimpleAggregateFunction(sum, Int64)) "
                        + "ENGINE = AggregatingMergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_simple_agg", table, "id BIGINT NOT NULL, total BIGINT NOT NULL"));
        env.executeSql("INSERT INTO ch_simple_agg VALUES (1, 10), (2, 20)").await();

        List<GenericRecord> rows = readBack("id, toInt64(total) AS total_v", table, "id", 2);
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals(10L, rows.get(0).getLong("total_v"));
        Assertions.assertEquals(20L, rows.get(1).getLong("total_v"));
    }

    @Test
    void updatingSourceIsRejectedAtPlanningAsInsertOnly() throws Exception {
        String table = "table_api_insert_only";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (name String, cnt Int64) ENGINE = MergeTree() ORDER BY name",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_insert_only", table, "name STRING NOT NULL, cnt BIGINT NOT NULL"));

        // A streaming GROUP BY emits updates; the insert-only sink must reject the plan (#148).
        Exception e = Assertions.assertThrows(Exception.class,
                () -> env.executeSql("INSERT INTO ch_insert_only "
                        + "SELECT name, COUNT(*) FROM (VALUES ('a'), ('a'), ('b')) AS t(name) "
                        + "GROUP BY name"));
        Assertions.assertTrue(exceptionChainContains(e, "doesn't support consuming update changes"),
                "Unexpected failure: " + e);
    }

    @Test
    void omittedClickHouseColumnsWithDefaultsAreBackfilled() throws Exception {
        String table = "table_api_defaults";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` ("
                        + "id Int64, note Nullable(String), tag String DEFAULT 'none'"
                        + ") ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_defaults", table, "id BIGINT NOT NULL"));
        env.executeSql("INSERT INTO ch_defaults VALUES (5)").await();

        List<GenericRecord> rows = readBack("id, ifNull(note, '<null>') AS note_s, tag", table, "id", 1);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(5L, rows.get(0).getLong("id"));
        Assertions.assertEquals("<null>", rows.get(0).getString("note_s"));
        Assertions.assertEquals("none", rows.get(0).getString("tag"));
    }

    @Test
    void omittedNoDefaultColumnIsFilledWithTheTypeDefault() throws Exception {
        String table = "table_api_no_default";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64, req String, tags Array(String)) "
                        + "ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_no_default", table, "id BIGINT NOT NULL"));
        env.executeSql("INSERT INTO ch_no_default VALUES (1)").await();

        List<GenericRecord> rows = readBack("id, req, length(tags) AS tags_len", table, "id", 1);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(1L, rows.get(0).getLong("id"));
        Assertions.assertEquals("", rows.get(0).getString("req"));
        Assertions.assertEquals(0L, rows.get(0).getLong("tags_len"));
    }

    @Test
    void statementSetInsertsIntoTheSameSinkTwice() throws Exception {
        String table = "table_api_stmt_set";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64, src String) ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_stmt_set", table, "id BIGINT NOT NULL, src STRING NOT NULL"));

        // Two INSERTs into one sink table: the planner copies the sink, exercising copy()'s deep copy.
        env.createStatementSet()
                .addInsertSql("INSERT INTO ch_stmt_set VALUES (1, 'first'), (2, 'first')")
                .addInsertSql("INSERT INTO ch_stmt_set VALUES (3, 'second'), (4, 'second')")
                .execute()
                .await();

        List<GenericRecord> rows = readBack("id, src", table, "id", 4);
        Assertions.assertEquals(4, rows.size());
        Assertions.assertEquals("first", rows.get(0).getString("src"));
        Assertions.assertEquals("first", rows.get(1).getString("src"));
        Assertions.assertEquals("second", rows.get(2).getString("src"));
        Assertions.assertEquals("second", rows.get(3).getString("src"));
    }

    @Test
    void columnsMapByNameNotPosition() throws Exception {
        String table = "table_api_permuted";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (a Int64, b String, c Float64) ENGINE = MergeTree() ORDER BY a",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        // Deliberately not the ClickHouse order; positional mapping could not even plan this.
        env.executeSql(sinkDdl("ch_permuted", table,
                "c DOUBLE NOT NULL, a BIGINT NOT NULL, b STRING NOT NULL"));
        env.executeSql("INSERT INTO ch_permuted VALUES (1.5, 7, 'x')").await();

        List<GenericRecord> rows = readBack("a, b, c", table, "a", 1);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(7L, rows.get(0).getLong("a"));
        Assertions.assertEquals("x", rows.get(0).getString("b"));
        Assertions.assertEquals(1.5, rows.get(0).getDouble("c"));
    }

    @Test
    void typeMismatchFailsAtPlanningNamingColumnAndBothTypes() throws Exception {
        String table = "table_api_mismatch";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64, label Int64) ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_mismatch", table, "id BIGINT NOT NULL, label STRING NOT NULL"));

        Exception e = Assertions.assertThrows(Exception.class,
                () -> env.executeSql("INSERT INTO ch_mismatch VALUES (1, 'nope')"));
        Assertions.assertTrue(exceptionChainContains(e,
                        "Column 'label': Flink type STRING NOT NULL cannot be written to "
                                + "ClickHouse column 'label Int64'"),
                "Unexpected failure: " + e);
    }

    @Test
    void ignoredUnknownFlinkColumnIsSkippedAtWriteTime() throws Exception {
        String table = "table_api_ignore_unknown";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64, name String) ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        // 'extra' sits between the mapped columns, so the surviving accessors must keep their indices.
        env.executeSql(sinkDdl("ch_ignore_unknown", table,
                "id BIGINT NOT NULL, extra STRING NOT NULL, name STRING NOT NULL",
                ", 'sink.ignore-unknown-flink-columns' = 'true'"));
        env.executeSql("INSERT INTO ch_ignore_unknown VALUES (3, 'dropped', 'carol')").await();

        List<GenericRecord> rows = readBack("id, name", table, "id", 1);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(3L, rows.get(0).getLong("id"));
        Assertions.assertEquals("carol", rows.get(0).getString("name"));
    }

    @Test
    void computedColumnsAreExcludedFromTheSinkSchema() throws Exception {
        String table = "table_api_computed";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64, name String) ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        // 'id_plus' has no ClickHouse counterpart and must never reach schema resolution.
        env.executeSql(sinkDdl("ch_computed", table,
                "id BIGINT NOT NULL, name STRING NOT NULL, id_plus AS id + 1"));
        env.executeSql("INSERT INTO ch_computed VALUES (1, 'x')").await();

        List<GenericRecord> rows = readBack("id, name", table, "id", 1);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(1L, rows.get(0).getLong("id"));
        Assertions.assertEquals("x", rows.get(0).getString("name"));
    }

    @Test
    void primaryKeyIsAcceptedAndIgnored() throws Exception {
        String table = "table_api_primary_key";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64, v String) ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_primary_key", table,
                "id BIGINT NOT NULL, v STRING NOT NULL, PRIMARY KEY (id) NOT ENFORCED"));
        // Same key twice: the sink appends both — no silent upsert until #148 makes it a choice.
        env.executeSql("INSERT INTO ch_primary_key VALUES (1, 'first'), (1, 'second')").await();

        List<GenericRecord> rows = readBack("id, v", table, "v", 2);
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals("first", rows.get(0).getString("v"));
        Assertions.assertEquals("second", rows.get(1).getString("v"));
    }

    @Test
    void sinkTimezoneInterpretsWallClockTimestamps() throws Exception {
        String table = "table_api_sink_timezone";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64, ts DateTime64(3)) ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_sink_tz", table,
                "id BIGINT NOT NULL, ts TIMESTAMP(3) NOT NULL",
                ", 'sink.timezone' = 'Asia/Tokyo'"));
        env.executeSql("INSERT INTO ch_sink_tz VALUES (1, TIMESTAMP '2026-01-02 09:00:00')").await();

        // 09:00 Tokyo wall clock is midnight UTC; compare instants, not rendered strings.
        List<GenericRecord> rows = readBack("id, toUnixTimestamp64Milli(ts) AS ts_ms", table, "id", 1);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(Instant.parse("2026-01-02T00:00:00Z").toEpochMilli(),
                rows.get(0).getLong("ts_ms"));
    }

    @Test
    void timestampLtzWritesTheInstantRegardlessOfZones() throws Exception {
        String table = "table_api_ltz";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64, ts DateTime64(3)) ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        // The session zone fixes the instant at CAST time; sink.timezone must not shift it again.
        env.getConfig().setLocalTimeZone(ZoneId.of("Asia/Tokyo"));
        env.executeSql(sinkDdl("ch_ltz", table,
                "id BIGINT NOT NULL, ts TIMESTAMP_LTZ(3) NOT NULL",
                ", 'sink.timezone' = 'America/New_York'"));
        env.executeSql("INSERT INTO ch_ltz VALUES "
                + "(1, CAST(TIMESTAMP '2026-01-02 09:00:00' AS TIMESTAMP_LTZ(3)))").await();

        List<GenericRecord> rows = readBack("id, toUnixTimestamp64Milli(ts) AS ts_ms", table, "id", 1);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(Instant.parse("2026-01-02T00:00:00Z").toEpochMilli(),
                rows.get(0).getLong("ts_ms"));
    }

    @Test
    void jsonColumnAcceptsJsonStrings() throws Exception {
        String table = "table_api_json";
        try {
            ClickHouseServerForTests.executeSql(String.format(
                    "CREATE TABLE `%s`.`%s` (id Int64, j JSON) ENGINE = MergeTree() ORDER BY id",
                    ClickHouseServerForTests.getDatabase(), table));
        } catch (Exception e) {
            // Probe, don't pin versions: the modern JSON type is GA from 25.3.
            Assumptions.assumeTrue(false, "Server lacks the JSON type: " + e.getMessage());
        }

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_json", table, "id BIGINT NOT NULL, j STRING NOT NULL"));
        env.executeSql("INSERT INTO ch_json VALUES (1, '{\"k\": \"v\", \"n\": 42}')").await();

        List<GenericRecord> rows = readBack(
                "id, toString(getSubcolumn(j, 'k')) AS k_s, toInt64(getSubcolumn(j, 'n')) AS n_v",
                table, "id", 1);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("v", rows.get(0).getString("k_s"));
        Assertions.assertEquals(42L, rows.get(0).getLong("n_v"));
    }

    /**
     * Fails when client-v2 learns to quote table names (clickhouse-java#3089) — then remove
     * SchemaResolverOptions#requireUnquotedTableName, its unit test, and this canary.
     */
    @Test
    void clientV2StillCannotDescribeTableNamesNeedingQuotes() throws Exception {
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`table-api-canary` (id Int64) ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase()));
        try (Client client = new Client.Builder()
                .addEndpoint(ClickHouseServerForTests.getURL())
                .setUsername(ClickHouseServerForTests.getUsername())
                .setPassword(ClickHouseServerForTests.getPassword())
                .build()) {
            Assertions.assertThrows(Exception.class, () -> client.getTableSchema(
                    "table-api-canary", ClickHouseServerForTests.getDatabase()));
        }
    }

    @Test
    void passthroughOptionsReachTheClientAndTheInsert() throws Exception {
        String table = "table_api_passthrough";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64) ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        // The client option must survive validation and client construction; the server setting is
        // recorded per query, so the insert's query_log row proves it travelled with the INSERT.
        env.executeSql(sinkDdl("ch_passthrough", table, "id BIGINT NOT NULL",
                ", 'clickhouse.client.socket_timeout' = '30000'"
                        + ", 'clickhouse.server.max_insert_block_size' = '777777'"));
        env.executeSql("INSERT INTO ch_passthrough VALUES (1)").await();

        Assertions.assertEquals(1, readBack("id", table, "id", 1).size());
        Assertions.assertTrue(insertsRecordedWithSetting(table, "max_insert_block_size", "777777") >= 1,
                "no query_log INSERT into " + table + " carries max_insert_block_size=777777");
    }

    @Test
    void barePassthroughPrefixIsRejectedAtPlanning() {
        TableEnvironment env = tableEnvironment();
        // The bare prefix passes FactoryUtil's prefix skip; it must fail here, not reach the server as '?=1'.
        env.executeSql(sinkDdl("ch_bare_prefix", "does_not_exist", "id BIGINT NOT NULL",
                ", 'clickhouse.server.' = '1'"));

        Exception e = Assertions.assertThrows(Exception.class,
                () -> env.executeSql("INSERT INTO ch_bare_prefix VALUES (1)"));
        Assertions.assertTrue(exceptionChainContains(e, "Option 'clickhouse.server.' has no key after the prefix"),
                "Unexpected failure: " + e);
    }

    /**
     * Read-back after {@code await()}. On Cloud the replica answering the SELECT may not yet see the
     * acknowledged insert, so poll (bounded) until the expected row count shows up; one read elsewhere.
     */
    private static List<GenericRecord> readBack(String columns, String table, String orderBy, int expectedRows)
            throws Exception {
        int attempts = ClickHouseServerForTests.isCloud() ? 30 : 1;
        List<GenericRecord> rows = ClickHouseServerForTests.extractData(
                columns, ClickHouseServerForTests.getDatabase(), table, orderBy);
        for (int i = 1; i < attempts && rows.size() != expectedRows; i++) {
            Thread.sleep(1000);
            rows = ClickHouseServerForTests.extractData(
                    columns, ClickHouseServerForTests.getDatabase(), table, orderBy);
        }
        return rows;
    }

    /** Finished INSERTs into {@code table} whose query_log row recorded {@code setting = value}. */
    private static long insertsRecordedWithSetting(String table, String setting, String value) throws Exception {
        boolean cloud = ClickHouseServerForTests.isCloud();
        ClickHouseServerForTests.executeSql(cloud ? "SYSTEM FLUSH LOGS ON CLUSTER 'default'" : "SYSTEM FLUSH LOGS");
        String sql = String.format(
                "SELECT count() FROM clusterAllReplicas('default', system.query_log) "
                        + "WHERE type = 'QueryFinish' AND query_kind = 'Insert' "
                        + "AND has(tables, '%s.%s') AND Settings['%s'] = '%s'",
                ClickHouseServerForTests.getDatabase(), table, setting, value);
        try (Client client = new Client.Builder()
                .addEndpoint(ClickHouseServerForTests.getURL())
                .setUsername(ClickHouseServerForTests.getUsername())
                .setPassword(ClickHouseServerForTests.getPassword())
                .build()) {
            long count = client.queryAll(sql).get(0).getLong(1);
            for (int i = 1; i < (cloud ? 30 : 5) && count < 1; i++) {
                Thread.sleep(1000);
                count = client.queryAll(sql).get(0).getLong(1);
            }
            return count;
        }
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
