package org.apache.flink.connector.clickhouse.table;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.query.GenericRecord;

import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseTestHelpers;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.ExceptionUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

    /** One subtask everywhere the DDL does not say otherwise, so INSERT counts are deterministic. */
    private static TableEnvironment singleParallelismEnvironment() {
        TableEnvironment env = tableEnvironment();
        env.getConfig().set("table.exec.resource.default-parallelism", "1");
        return env;
    }

    private static String valuesList(int rows) {
        return IntStream.rangeClosed(1, rows).mapToObj(i -> "(" + i + ")").collect(Collectors.joining(", "));
    }

    /** {@code parsed} is omitted by the Flink schema, so the server evaluates its DEFAULT per row. */
    private static void createTableWithParsedDefault(String table) throws Exception {
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64, src String, parsed Int32 DEFAULT toInt32(src)) "
                        + "ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));
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

    /** DESCRIBE pretty-prints named Tuples across lines by default; the insert header must carry the canonical type. */
    @Test
    void namedTuplesAtEveryNestingRoundTrip() throws Exception {
        String table = "table_api_named_tuple";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` ("
                        + "id Int64, pair Tuple(a Int32, b String), pairs Array(Tuple(a Int32, b String)), "
                        + "by_key Map(String, Tuple(a Int32, b String))"
                        + ") ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_named_tuple", table,
                "id BIGINT NOT NULL,"
                        + "pair ROW<a INT NOT NULL, b STRING NOT NULL> NOT NULL,"
                        + "pairs ARRAY<ROW<a INT NOT NULL, b STRING NOT NULL> NOT NULL> NOT NULL,"
                        + "by_key MAP<STRING, ROW<a INT NOT NULL, b STRING NOT NULL> NOT NULL> NOT NULL"));
        env.executeSql("INSERT INTO ch_named_tuple VALUES "
                + "(1, ROW(7, 'x'), ARRAY[ROW(1, 'p'), ROW(2, 'q')], MAP['k', ROW(5, 'z')])").await();

        List<GenericRecord> rows = readBack(
                "id, toString(pair) AS pair_s, toString(pairs) AS pairs_s, toString(by_key['k']) AS k_s",
                table, "id", 1);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("(7,'x')", rows.get(0).getString("pair_s"));
        Assertions.assertEquals("[(1,'p'),(2,'q')]", rows.get(0).getString("pairs_s"));
        Assertions.assertEquals("(5,'z')", rows.get(0).getString("k_s"));
    }

    /** Planning admits a nullable value into a NOT NULL nested field; the write must fail, not store zeros. */
    @Test
    void nullNestedValueFailsNamingTheColumnInsteadOfWritingZeros() throws Exception {
        String table = "table_api_nested_null";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64, pair Tuple(Int32, String), nums Array(Int32)) "
                        + "ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_nested_null", table,
                "id BIGINT NOT NULL,"
                        + "pair ROW<a INT NOT NULL, b STRING NOT NULL> NOT NULL,"
                        + "nums ARRAY<INT NOT NULL> NOT NULL"));

        Exception rowFailure = Assertions.assertThrows(Exception.class, () -> env.executeSql(
                "INSERT INTO ch_nested_null VALUES (1, ROW(CAST(NULL AS INT), 'x'), ARRAY[1])").await());
        Assertions.assertTrue(exceptionChainContains(rowFailure, "Column 'pair': null ROW field 1"),
                "Unexpected failure: " + rowFailure);
        Exception arrayFailure = Assertions.assertThrows(Exception.class, () -> env.executeSql(
                "INSERT INTO ch_nested_null VALUES (2, ROW(7, 'x'), ARRAY[1, CAST(NULL AS INT)])").await());
        Assertions.assertTrue(exceptionChainContains(arrayFailure, "Column 'nums': null array element 2"),
                "Unexpected failure: " + arrayFailure);
        Assertions.assertEquals(0, readBack("id", table, "id", 0).size());
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
            // Probe, don't pin versions: the modern JSON type is GA from 25.3. Only the server's own
            // "no JSON type" answer may skip; a connection or DDL problem must fail the test.
            Assumptions.assumeFalse(lacksJsonType(e), "Server lacks the JSON type: " + e.getMessage());
            throw e;
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
        try (Client client = fixtureClient()) {
            Exception e = Assertions.assertThrows(Exception.class, () -> client.getTableSchema(
                    "table-api-canary", ClickHouseServerForTests.getDatabase()));
            // Unquoted, the name parses as `table - api - canary`: a server SYNTAX_ERROR (62), not
            // some other DESCRIBE failure such as the 26.8 header-format break.
            ServerException server = ExceptionUtils.findThrowable(e, ServerException.class)
                    .orElseThrow(() -> new AssertionError("expected a server-side syntax error, got: " + e, e));
            Assertions.assertEquals(62, server.getCode(), server.getMessage());
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

    @Test
    void zeroScaleDecimalsWriteIntoTheIntegersTheirDigitsCover() throws Exception {
        String table = "table_api_decimal_ints";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64, small UInt8, big Int64) ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_decimal_ints", table,
                "id BIGINT NOT NULL, small DECIMAL(3, 0) NOT NULL, big DECIMAL(18, 0) NOT NULL"));
        env.executeSql("INSERT INTO ch_decimal_ints VALUES "
                + "(1, CAST(255 AS DECIMAL(3, 0)), CAST(123456789012345678 AS DECIMAL(18, 0)))").await();

        List<GenericRecord> rows = readBack("id, small, big", table, "id", 1);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(255, rows.get(0).getInteger("small"));
        Assertions.assertEquals(123456789012345678L, rows.get(0).getLong("big"));
    }

    @Test
    void sinkParallelismSplitsTheInsertAcrossThatManyWriters() throws Exception {
        String table = "table_api_parallelism";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64) ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = singleParallelismEnvironment();
        // Each writer subtask flushes its share once at end of input, so the INSERT count is the
        // writer count; the interval is pinned high so a slow run cannot add a timer flush.
        env.executeSql(sinkDdl("ch_parallel", table, "id BIGINT NOT NULL",
                ", 'sink.parallelism' = '2', 'sink.buffer-flush.interval' = '10 min'"));
        env.executeSql("INSERT INTO ch_parallel VALUES " + valuesList(10)).await();

        Assertions.assertEquals(10, readBack("id", table, "id", 10).size());
        Assertions.assertEquals(2, finishedInserts(table, "", 2),
                "expected exactly one INSERT per writer subtask");
    }

    @Test
    void bufferFlushIntervalFlushesAnUnboundedStreamOnTime() throws Exception {
        String table = "table_api_interval";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64) ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = singleParallelismEnvironment();
        env.executeSql("CREATE TABLE gen (id BIGINT NOT NULL) WITH ("
                + "'connector' = 'datagen', 'rows-per-second' = '10')");
        // Rows and bytes can never trigger a flush here, so only the 1 s timer moves data.
        env.executeSql(sinkDdl("ch_interval", table, "id BIGINT NOT NULL",
                ", 'sink.buffer-flush.interval' = '1s', 'sink.buffer-flush.max-rows' = '5000'"));

        JobClient job = env.executeSql("INSERT INTO ch_interval SELECT id FROM gen")
                .getJobClient().orElseThrow(() -> new AssertionError("no job client"));
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            int rows = ClickHouseServerForTests.countRows(table);
            while (rows < 40 && System.nanoTime() < deadline) {
                Thread.sleep(250);
                rows = ClickHouseServerForTests.countRows(table);
            }
            Assertions.assertTrue(rows >= 40, "only " + rows + " rows landed within 90 s");
        } finally {
            job.cancel().get(60, TimeUnit.SECONDS);
        }
        // At 10 rows/s a 1 s timer lands ~10 rows per INSERT; the 5 s default would have put the
        // first ~50 rows into a single INSERT.
        long inserts = finishedInserts(table, "", 2);
        Assertions.assertTrue(inserts >= 2, "40 rows arrived in " + inserts + " INSERT(s)");
    }

    @Test
    void singleInFlightRequestSerialisesTheInserts() throws Exception {
        String table = "table_api_in_flight";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64) ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = singleParallelismEnvironment();
        // One row per batch gives 20 INSERTs; the default of 50 in flight would overlap them.
        env.executeSql(sinkDdl("ch_in_flight", table, "id BIGINT NOT NULL",
                ", 'sink.buffer-flush.max-rows' = '1', 'sink.max-in-flight-requests' = '1'"));
        env.executeSql("INSERT INTO ch_in_flight VALUES " + valuesList(20)).await();

        Assertions.assertEquals(20, readBack("id", table, "id", 20).size());
        Assertions.assertEquals(20, finishedInserts(table, "", 20));
        if (!ClickHouseServerForTests.isCloud()) {
            // Replicas keep separate clocks, so only a single server can time-order the INSERTs.
            Assertions.assertEquals(0, overlappingInserts(table), "INSERTs overlapped on the server");
        }
    }

    @Test
    void maxBufferedRequestsNotAboveBatchRowsIsRejectedAtPlanning() {
        TableEnvironment env = tableEnvironment();
        // Below the 500-row sink.buffer-flush.max-rows default.
        env.executeSql(sinkDdl("ch_small_buffer", "does_not_exist", "id BIGINT NOT NULL",
                ", 'sink.max-buffered-requests' = '400'"));

        Exception e = Assertions.assertThrows(Exception.class,
                () -> env.executeSql("INSERT INTO ch_small_buffer VALUES (1)"));
        Assertions.assertTrue(exceptionChainContains(e,
                        "'sink.max-buffered-requests' (400) must be strictly greater than "
                                + "'sink.buffer-flush.max-rows' (500)"),
                "Unexpected failure: " + e);
    }

    @Test
    void twoEntryBufferBackpressuresWithoutLosingRows() throws Exception {
        String table = "table_api_tiny_buffer";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64) ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = singleParallelismEnvironment();
        // With one row per batch and one request in flight, the third row must block until the
        // first INSERT is acknowledged; the job has to drain instead of hanging or dropping rows.
        env.executeSql(sinkDdl("ch_tiny_buffer", table, "id BIGINT NOT NULL",
                ", 'sink.buffer-flush.max-rows' = '1', 'sink.max-in-flight-requests' = '1'"
                        + ", 'sink.max-buffered-requests' = '2'"));
        env.executeSql("INSERT INTO ch_tiny_buffer VALUES " + valuesList(20)).await();

        List<GenericRecord> rows = readBack("id", table, "id", 20);
        Assertions.assertEquals(20, rows.size());
        Assertions.assertEquals(20L, rows.get(19).getLong("id"));
    }

    @Test
    void oversizedRecordFailsNamingTheRecordLimit() throws Exception {
        String table = "table_api_record_bytes";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64, payload String) ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_record_bytes", table, "id BIGINT NOT NULL, payload STRING NOT NULL",
                ", 'sink.record.max-bytes' = '64b'"));
        // A 13-byte RowBinary row fits; a 200-char payload is rejected by the AsyncSink writer.
        env.executeSql("INSERT INTO ch_record_bytes VALUES (1, 'tiny')").await();
        Assertions.assertEquals(1, readBack("id", table, "id", 1).size());

        Exception e = Assertions.assertThrows(Exception.class,
                () -> env.executeSql("INSERT INTO ch_record_bytes VALUES (2, REPEAT('x', 200))").await());
        Assertions.assertTrue(exceptionChainContains(e, "maxRecordSizeInBytes was set to [64]"),
                "Unexpected failure: " + e);
    }

    @Test
    void dropBatchStrategyKeepsGoodBatchesAndDropsTheRejectedOne() throws Exception {
        String table = "table_api_drop_batch";
        createTableWithParsedDefault(table);

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_drop_batch", table, "id BIGINT NOT NULL, src STRING NOT NULL",
                ", 'sink.batch-failure-strategy' = 'drop-batch', 'sink.buffer-flush.max-rows' = '1'"));
        // The server's parse failure (code 6) is data corruption, so the middle batch is dropped.
        env.executeSql("INSERT INTO ch_drop_batch VALUES (1, '10'), (2, 'not-a-number'), (3, '30')").await();

        List<GenericRecord> rows = readBack("id, parsed", table, "id", 2);
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals(1L, rows.get(0).getLong("id"));
        Assertions.assertEquals(10, rows.get(0).getInteger("parsed"));
        Assertions.assertEquals(3L, rows.get(1).getLong("id"));
        Assertions.assertEquals(30, rows.get(1).getInteger("parsed"));
    }

    @Test
    void stopFlinkStrategyFailsTheJobOnARejectedBatch() throws Exception {
        String table = "table_api_stop_flink";
        createTableWithParsedDefault(table);

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_stop_flink", table, "id BIGINT NOT NULL, src STRING NOT NULL",
                ", 'sink.batch-failure-strategy' = 'stop-flink', 'sink.buffer-flush.max-rows' = '1'"));

        Exception e = Assertions.assertThrows(Exception.class,
                () -> env.executeSql("INSERT INTO ch_stop_flink VALUES (1, '10'), (2, 'not-a-number')").await());
        Assertions.assertTrue(exceptionChainContains(e, "not-a-number"), "Unexpected failure: " + e);
    }

    @Test
    void ephemeralColumnsAreRejectedAtPlanning() throws Exception {
        String table = "table_api_ephemeral";
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (id Int64, payload String EPHEMERAL, norm String DEFAULT lower(payload)) "
                        + "ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table));

        TableEnvironment env = tableEnvironment();
        // The sink's INSERT carries no column list, so a header naming 'payload' would be dropped silently.
        env.executeSql(sinkDdl("ch_ephemeral", table, "id BIGINT NOT NULL, payload STRING NOT NULL"));

        Exception e = Assertions.assertThrows(Exception.class,
                () -> env.executeSql("INSERT INTO ch_ephemeral VALUES (1, 'X')"));
        Assertions.assertTrue(exceptionChainContains(e, "is EPHEMERAL"), "Unexpected failure: " + e);
        Assertions.assertTrue(exceptionChainContains(e, "without a column list"), "Unexpected failure: " + e);
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
        return finishedInserts(table, String.format(" AND Settings['%s'] = '%s'", setting, value), 1);
    }

    /**
     * Finished INSERTs into {@code table} matching {@code extraWhere}, polled (bounded) until at
     * least {@code atLeast} are visible — query_log lands asynchronously, and later on Cloud.
     */
    private static long finishedInserts(String table, String extraWhere, long atLeast) throws Exception {
        boolean cloud = ClickHouseServerForTests.isCloud();
        ClickHouseServerForTests.executeSql(cloud ? "SYSTEM FLUSH LOGS ON CLUSTER 'default'" : "SYSTEM FLUSH LOGS");
        String sql = "SELECT count() FROM " + finishedInsertsInto(table) + extraWhere;
        try (Client client = fixtureClient()) {
            long count = client.queryAll(sql).get(0).getLong(1);
            for (int i = 1; i < (cloud ? 30 : 5) && count < atLeast; i++) {
                Thread.sleep(1000);
                count = client.queryAll(sql).get(0).getLong(1);
            }
            return count;
        }
    }

    /** Pairs of finished INSERTs into {@code table} where the later one started before the earlier one finished. */
    private static long overlappingInserts(String table) throws Exception {
        String sql = String.format(
                "SELECT count() FROM (SELECT query_start_time_microseconds AS s, event_time_microseconds AS e "
                        + "FROM %1$s) AS a, (SELECT query_start_time_microseconds AS s FROM %1$s) AS b "
                        + "WHERE a.s < b.s AND b.s < a.e",
                finishedInsertsInto(table));
        try (Client client = fixtureClient()) {
            return client.queryAll(sql).get(0).getLong(1);
        }
    }

    private static String finishedInsertsInto(String table) {
        return String.format(
                "clusterAllReplicas('default', system.query_log) WHERE type = 'QueryFinish' "
                        + "AND query_kind = 'Insert' AND has(tables, '%s.%s')",
                ClickHouseServerForTests.getDatabase(), table);
    }

    /** The client the fixture itself uses: 60 s connect timeout, TLS exactly when on Cloud. */
    private static Client fixtureClient() {
        return ClickHouseTestHelpers.getClient(
                ClickHouseServerForTests.getHost(), ClickHouseServerForTests.getPort(),
                ClickHouseServerForTests.isCloud(),
                ClickHouseServerForTests.getUsername(), ClickHouseServerForTests.getPassword());
    }

    /** Only the server's own "JSON type unavailable" answers: an experimental gate (24.x spells it Object('json')) or an unknown type. */
    private static boolean lacksJsonType(Throwable t) {
        return ExceptionUtils.findThrowable(t, ServerException.class)
                .map(e -> e.getMessage().toLowerCase(Locale.ROOT))
                .filter(m -> m.contains("json") && (m.contains("not allowed") || m.contains("unknown data type")))
                .isPresent();
    }

    private static boolean exceptionChainContains(Throwable t, String needle) {
        return ExceptionUtils.findThrowableWithMessage(t, needle).isPresent();
    }
}
