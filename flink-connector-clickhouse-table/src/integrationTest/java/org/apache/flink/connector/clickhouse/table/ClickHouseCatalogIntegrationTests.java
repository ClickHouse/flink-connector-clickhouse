package org.apache.flink.connector.clickhouse.table;

import com.clickhouse.client.api.query.GenericRecord;

import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.util.ExceptionUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/** The catalog against a real ClickHouse: browse it, then insert with no per-table DDL. */
public class ClickHouseCatalogIntegrationTests {

    @BeforeAll
    public static void setUp() throws Exception {
        ClickHouseServerForTests.setUp();
    }

    @AfterAll
    public static void tearDown() {
        ClickHouseServerForTests.tearDown();
    }

    /** A session with the catalog registered as {@code ch}, its default database the test database. */
    private static TableEnvironment environmentWithCatalog(String... extraOptions) {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.executeSql(createCatalog(ClickHouseServerForTests.getURL(), ClickHouseServerForTests.getPassword(),
                ClickHouseServerForTests.getDatabase(), extraOptions));
        return env;
    }

    private static String createCatalog(String url, String password, String defaultDatabase, String... extraOptions) {
        StringBuilder ddl = new StringBuilder(String.format(
                "CREATE CATALOG ch WITH ('type' = 'clickhouse', 'url' = '%s', 'username' = '%s', "
                        + "'password' = '%s', 'default-database' = '%s'",
                url, ClickHouseServerForTests.getUsername(), password, defaultDatabase));
        for (String option : extraOptions) {
            ddl.append(", ").append(option);
        }
        return ddl.append(")").toString();
    }

    /** {@code ch.`<test database>`.<table>}; the database is quoted because {@code default} is a keyword. */
    private static String inCatalog(String table) {
        return "ch.`" + ClickHouseServerForTests.getDatabase() + "`." + table;
    }

    private static void createTable(String table, String columns) throws Exception {
        ClickHouseServerForTests.executeSql(String.format(
                "CREATE TABLE `%s`.`%s` (%s) ENGINE = MergeTree() ORDER BY id",
                ClickHouseServerForTests.getDatabase(), table, columns));
    }

    /** On Cloud the replica answering may not have the acknowledged insert yet, so poll there. */
    private static List<GenericRecord> readBack(String columns, String database, String table, int expectedRows)
            throws Exception {
        int attempts = ClickHouseServerForTests.isCloud() ? 30 : 1;
        List<GenericRecord> rows = ClickHouseServerForTests.extractData(columns, database, table, "id");
        for (int i = 1; i < attempts && rows.size() != expectedRows; i++) {
            Thread.sleep(1000);
            rows = ClickHouseServerForTests.extractData(columns, database, table, "id");
        }
        return rows;
    }

    private static List<GenericRecord> readBack(String columns, String table, int expectedRows) throws Exception {
        return readBack(columns, ClickHouseServerForTests.getDatabase(), table, expectedRows);
    }

    /** Asserts the call fails with every needle somewhere in the exception chain. */
    private static void assertFailsWith(Executable call, String... needles) {
        Exception e = Assertions.assertThrows(Exception.class, call);
        for (String needle : needles) {
            Assertions.assertTrue(ExceptionUtils.findThrowableWithMessage(e, needle).isPresent(),
                    "Expected '" + needle + "' in: " + ExceptionUtils.stringifyException(e));
        }
    }

    private static List<String> columnsOf(TableEnvironment env, String tablePath) {
        ResolvedSchema schema = env.from(tablePath).getResolvedSchema();
        return schema.getColumns().stream()
                .map(column -> column.getName() + " " + column.getDataType().getLogicalType().asSummaryString())
                .collect(Collectors.toList());
    }

    @Test
    void theCatalogShowsTheDatabasesAndTablesOfClickHouse() throws Exception {
        createTable("catalog_listed_a", "id Int64");
        createTable("catalog_listed_b", "id Int64");

        TableEnvironment env = environmentWithCatalog();
        env.useCatalog("ch");

        Assertions.assertEquals(ClickHouseServerForTests.getDatabase(), env.getCurrentDatabase());
        List<String> databases = Arrays.asList(env.listDatabases());
        Assertions.assertTrue(databases.contains(ClickHouseServerForTests.getDatabase()), databases.toString());
        Assertions.assertTrue(databases.contains("system"), databases.toString());
        List<String> tables = Arrays.asList(env.listTables());
        Assertions.assertTrue(tables.containsAll(List.of("catalog_listed_a", "catalog_listed_b")), tables.toString());

        // A table created in ClickHouse afterwards shows without re-creating the catalog.
        createTable("catalog_listed_later", "id Int64");
        Assertions.assertTrue(Arrays.asList(env.listTables()).contains("catalog_listed_later"));
    }

    /** The schema is derived and the sink options are pre-filled, so the INSERT needs nothing declared. */
    @Test
    void insertThroughTheCatalogNeedsNoTableDdl() throws Exception {
        String table = "catalog_events";
        createTable(table,
                "id Int64, name String, amount Decimal(18, 4), created_at DateTime64(3), "
                        + "seen_at DateTime64(3, 'UTC'), uid UUID, event_day Date, is_active Bool, score Float64, "
                        + "tags Array(String), props Map(String, String), category LowCardinality(String), "
                        + "code FixedString(4), note Nullable(String), small UInt8, big UInt64, "
                        + "pair Tuple(a Int32, b String), counts Array(Nullable(Int32))");

        TableEnvironment env = environmentWithCatalog();

        Assertions.assertEquals(List.of(
                        "id BIGINT NOT NULL",
                        "name STRING NOT NULL",
                        "amount DECIMAL(18, 4) NOT NULL",
                        "created_at TIMESTAMP(3) NOT NULL",
                        "seen_at TIMESTAMP_LTZ(3) NOT NULL",
                        "uid STRING NOT NULL",
                        "event_day DATE NOT NULL",
                        "is_active BOOLEAN NOT NULL",
                        "score DOUBLE NOT NULL",
                        "tags ARRAY<STRING NOT NULL> NOT NULL",
                        "props MAP<STRING, STRING NOT NULL> NOT NULL",
                        "category STRING NOT NULL",
                        "code STRING NOT NULL",
                        "note STRING",
                        "small SMALLINT NOT NULL",
                        "big DECIMAL(20, 0) NOT NULL",
                        "pair ROW<`a` INT NOT NULL, `b` STRING NOT NULL> NOT NULL",
                        "counts ARRAY<INT> NOT NULL"),
                columnsOf(env, inCatalog(table)));

        long seenAt = Instant.parse("2026-01-02T03:04:05.678Z").toEpochMilli();
        env.executeSql("INSERT INTO " + inCatalog(table) + " VALUES "
                + "(1, 'alice', CAST(12.5 AS DECIMAL(18, 4)), TIMESTAMP '2026-01-02 03:04:05.678', "
                + "TO_TIMESTAMP_LTZ(" + seenAt + ", 3), 'f47ac10b-58cc-4372-a567-0e02b2c3d479', "
                + "DATE '2026-01-02', true, 99.25, ARRAY['a', 'b'], MAP['k1', 'v1'], 'gold', 'AB12', "
                + "CAST(NULL AS STRING), CAST(255 AS SMALLINT), CAST('18446744073709551615' AS DECIMAL(20, 0)), "
                + "ROW(7, 'seven'), ARRAY[1, CAST(NULL AS INT)])").await();

        List<GenericRecord> rows = readBack(
                "id, name, amount, toString(created_at) AS created_at_s, toString(seen_at) AS seen_at_s, "
                        + "toString(uid) AS uid_s, toString(event_day) AS day_s, is_active, score, "
                        + "toString(tags) AS tags_s, toString(props) AS props_s, category, toString(code) AS code_s, "
                        + "isNull(note) AS note_is_null, toString(small) AS small_s, toString(big) AS big_s, "
                        + "toString(pair) AS pair_s, toString(counts) AS counts_s",
                table, 1);

        Assertions.assertEquals(1, rows.size());
        GenericRecord row = rows.get(0);
        Assertions.assertEquals(1L, row.getLong("id"));
        Assertions.assertEquals("alice", row.getString("name"));
        Assertions.assertEquals(0, new BigDecimal("12.5").compareTo(row.getBigDecimal("amount")));
        Assertions.assertEquals("2026-01-02 03:04:05.678", row.getString("created_at_s"));
        Assertions.assertEquals("2026-01-02 03:04:05.678", row.getString("seen_at_s"));
        Assertions.assertEquals("f47ac10b-58cc-4372-a567-0e02b2c3d479", row.getString("uid_s"));
        Assertions.assertEquals("2026-01-02", row.getString("day_s"));
        Assertions.assertTrue(row.getBoolean("is_active"));
        Assertions.assertEquals(99.25, row.getDouble("score"));
        Assertions.assertEquals("['a','b']", row.getString("tags_s"));
        Assertions.assertEquals("{'k1':'v1'}", row.getString("props_s"));
        Assertions.assertEquals("gold", row.getString("category"));
        Assertions.assertEquals("AB12", row.getString("code_s"));
        Assertions.assertTrue(row.getBoolean("note_is_null"));
        Assertions.assertEquals("255", row.getString("small_s"));
        Assertions.assertEquals("18446744073709551615", row.getString("big_s"));
        Assertions.assertEquals("(7,'seven')", row.getString("pair_s"));
        Assertions.assertEquals("[1,NULL]", row.getString("counts_s"));
    }

    /** The everyday shape: a query over a source whose columns are nullable, as Flink DDL makes them. */
    @Test
    void aQueryOverANullableSourceInsertsIntoTheDefaultDatabase() throws Exception {
        String table = "catalog_from_source";
        createTable(table, "id Int64, name String");

        TableEnvironment env = environmentWithCatalog();
        env.executeSql("CREATE TEMPORARY TABLE numbers (id BIGINT, name STRING) WITH ("
                + "'connector' = 'datagen', 'number-of-rows' = '5', 'fields.id.kind' = 'sequence', "
                + "'fields.id.start' = '1', 'fields.id.end' = '5', 'fields.name.length' = '4')");
        env.executeSql("USE CATALOG ch");

        // With ch current, the planner also asks the catalog for the function before finding the built-in.
        env.executeSql("INSERT INTO " + table
                + " SELECT id, UPPER(name) FROM default_catalog.default_database.numbers").await();

        List<GenericRecord> rows = readBack("id, name", table, 5);
        Assertions.assertEquals(List.of(1L, 2L, 3L, 4L, 5L),
                rows.stream().map(r -> r.getLong("id")).collect(Collectors.toList()));
        String name = rows.get(0).getString("name");
        Assertions.assertEquals(4, name.length());
        Assertions.assertEquals(name.toUpperCase(Locale.ROOT), name);
    }

    /** No insert can carry these columns, so the schema leaves them out and the server fills them. */
    @Test
    void serverComputedColumnsAreLeftOutAndStillFilledByTheServer() throws Exception {
        String table = "catalog_computed";
        createTable(table,
                "id Int64, src String, label String DEFAULT 'none', parsed Int32 MATERIALIZED toInt32(src), "
                        + "doubled Int64 ALIAS id * 2, hint String EPHEMERAL");

        TableEnvironment env = environmentWithCatalog();

        Assertions.assertEquals(List.of("id BIGINT NOT NULL", "src STRING NOT NULL", "label STRING NOT NULL"),
                columnsOf(env, inCatalog(table)));
        env.executeSql("INSERT INTO " + inCatalog(table) + " VALUES (21, '42', 'given')").await();

        List<GenericRecord> rows = readBack("id, label, parsed, doubled", table, 1);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("given", rows.get(0).getString("label"));
        Assertions.assertEquals(42, rows.get(0).getInteger("parsed"));
        Assertions.assertEquals(42L, rows.get(0).getLong("doubled"));
    }

    /** One unwritable column fails the table; the hand-declared table stays the way around it. */
    @Test
    void aColumnTheSinkCannotWriteFailsTheTableNamingIt() throws Exception {
        String table = "catalog_enum";
        createTable(table, "id Int64, status Enum8('new' = 1, 'done' = 2) DEFAULT 'new'");

        TableEnvironment env = environmentWithCatalog();

        assertFailsWith(() -> env.executeSql("INSERT INTO " + inCatalog(table) + " VALUES (1)"),
                "'status Enum8('new' = 1, 'done' = 2)'", table, "see issue #43", "CREATE TEMPORARY TABLE");

        env.executeSql(String.format(
                "CREATE TEMPORARY TABLE enum_by_hand (id BIGINT NOT NULL) WITH ('connector' = 'clickhouse', "
                        + "'url' = '%s', 'username' = '%s', 'password' = '%s', 'database' = '%s', 'table' = '%s')",
                ClickHouseServerForTests.getURL(), ClickHouseServerForTests.getUsername(),
                ClickHouseServerForTests.getPassword(), ClickHouseServerForTests.getDatabase(), table));
        env.executeSql("INSERT INTO enum_by_hand VALUES (1)").await();

        List<GenericRecord> rows = readBack("id, toString(status) AS status_s", table, 1);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("new", rows.get(0).getString("status_s"));
    }

    @Test
    void everyMutationIsRefusedAndClickHouseIsLeftAlone() throws Exception {
        String table = "catalog_read_only";
        createTable(table, "id Int64");
        TableEnvironment env = environmentWithCatalog();

        assertFailsWith(() -> env.executeSql("CREATE TABLE " + inCatalog("catalog_made_in_flink")
                + " (id BIGINT) WITH ('connector' = 'blackhole')"), "catalog 'ch' is read-only", "CREATE TABLE");
        assertFailsWith(() -> env.executeSql("DROP TABLE " + inCatalog(table)), "catalog 'ch' is read-only", "DROP TABLE");
        assertFailsWith(() -> env.executeSql("CREATE DATABASE ch.catalog_made_in_flink"),
                "catalog 'ch' is read-only", "CREATE DATABASE");

        env.useCatalog("ch");
        List<String> tables = Arrays.asList(env.listTables());
        Assertions.assertTrue(tables.contains(table), tables.toString());
        Assertions.assertFalse(tables.contains("catalog_made_in_flink"), tables.toString());
        Assertions.assertFalse(Arrays.asList(env.listDatabases()).contains("catalog_made_in_flink"));
    }

    @Test
    void aMissingTableIsReportedByThePlanner() {
        TableEnvironment env = environmentWithCatalog();

        assertFailsWith(() -> env.executeSql("INSERT INTO " + inCatalog("catalog_no_such_table") + " VALUES (1)"),
                "catalog_no_such_table");
        assertFailsWith(() -> env.executeSql("INSERT INTO ch.catalog_no_such_database.t VALUES (1)"),
                "catalog_no_such_database");
    }

    /** CREATE CATALOG opens the catalog, which is where a connection that cannot work must surface. */
    @Test
    void createCatalogFailsOnAConnectionThatCannotWork() {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        String url = ClickHouseServerForTests.getURL();
        String password = ClickHouseServerForTests.getPassword();
        String database = ClickHouseServerForTests.getDatabase();

        // Port 1 never listens.
        assertFailsWith(() -> env.executeSql(createCatalog("http://localhost:1", password, database)),
                "Could not read the metadata of ClickHouse at http://localhost:1", "Connection refused");
        assertFailsWith(() -> env.executeSql(createCatalog(url, password + "-wrong", database)),
                "Could not read the metadata of ClickHouse", "Authentication failed");
        assertFailsWith(() -> env.executeSql(createCatalog(url, password, "catalog_no_such_database")),
                "Default database 'catalog_no_such_database'", "'default-database'");
        Assertions.assertFalse(Arrays.asList(env.listCatalogs()).contains("ch"));
    }

    /** A sink option set on the catalog reaches every table, and an OPTIONS hint still overrides it per statement. */
    @Test
    void catalogSinkOptionsApplyToItsTablesAndOptionsHintsOverrideThem() throws Exception {
        String table = "catalog_strict";
        createTable(table, "id Int64, small UInt8");
        TableEnvironment env = environmentWithCatalog("'sink.strict-numeric-mapping' = 'true'");

        assertFailsWith(() -> env.executeSql(
                        "INSERT INTO " + inCatalog(table) + " VALUES (1, CAST(7 AS SMALLINT))"),
                "Column 'small'", "'sink.strict-numeric-mapping'");

        env.executeSql("INSERT INTO " + inCatalog(table)
                + " /*+ OPTIONS('sink.strict-numeric-mapping' = 'false') */ VALUES (1, CAST(7 AS SMALLINT))").await();

        List<GenericRecord> rows = readBack("id, toString(small) AS small_s", table, 1);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("7", rows.get(0).getString("small_s"));
    }

    /** Visible and writable, not readable: the connector has no table source. */
    @Test
    void selectThroughTheCatalogFailsForWantOfASource() throws Exception {
        String table = "catalog_no_source";
        createTable(table, "id Int64");
        TableEnvironment env = environmentWithCatalog();

        assertFailsWith(() -> env.executeSql("SELECT * FROM " + inCatalog(table)).await(),
                "can only be used as a sink");
    }

    /** The client cannot address such a table, so the catalog lists it and says why it cannot be used. */
    @Test
    void aTableNameNeedingQuotesIsListedButRefused() throws Exception {
        createTable("catalog-dashed", "id Int64");
        TableEnvironment env = environmentWithCatalog();

        Assertions.assertTrue(Arrays.asList(
                env.listTables("ch", ClickHouseServerForTests.getDatabase())).contains("catalog-dashed"));
        assertFailsWith(() -> env.executeSql("INSERT INTO " + inCatalog("`catalog-dashed`") + " VALUES (1)"),
                "catalog-dashed", "needs quoting");
    }

    /** A database name travels as a bound parameter or a request setting, never spliced into SQL. */
    @Test
    void aDatabaseNameNeedingQuotesWorksEndToEnd() throws Exception {
        String database = ClickHouseServerForTests.getDatabase() + "-o'neil";
        ClickHouseServerForTests.executeSql("CREATE DATABASE `" + database + "`");
        try {
            ClickHouseServerForTests.executeSql(
                    "CREATE TABLE `" + database + "`.`events` (id Int64, name String) ENGINE = MergeTree() ORDER BY id");
            TableEnvironment env = environmentWithCatalog();
            env.useCatalog("ch");

            Assertions.assertTrue(Arrays.asList(env.listDatabases()).contains(database));
            Assertions.assertEquals(List.of("events"), Arrays.asList(env.listTables("ch", database)));
            env.executeSql("INSERT INTO ch.`" + database + "`.events VALUES (1, 'one')").await();

            List<GenericRecord> rows = readBack("id, name", database, "events", 1);
            Assertions.assertEquals(1, rows.size());
            Assertions.assertEquals("one", rows.get(0).getString("name"));
        } finally {
            ClickHouseServerForTests.executeSql("DROP DATABASE IF EXISTS `" + database + "` SYNC");
        }
    }
}
