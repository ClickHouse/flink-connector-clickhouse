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
import org.junit.jupiter.api.function.Executable;

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
        createTable(table, "id Int64, src String, parsed Int32 DEFAULT toInt32(src)");
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

    /** {@code CREATE TABLE} in the test database; the default engine clause is {@code MergeTree() ORDER BY id}. */
    private static void createTable(String table, String columns) throws Exception {
        createTable(table, columns, "MergeTree() ORDER BY id");
    }

    private static void createTable(String table, String columns, String engineClause) throws Exception {
        ClickHouseServerForTests.executeSql(String.format("CREATE TABLE `%s`.`%s` (%s) ENGINE = %s",
                ClickHouseServerForTests.getDatabase(), table, columns, engineClause));
    }

    /** Asserts the call fails with every needle somewhere in the exception chain; returns the exception. */
    private static Exception assertFailsWith(Executable call, String... needles) {
        Exception e = Assertions.assertThrows(Exception.class, call);
        for (String needle : needles) {
            Assertions.assertTrue(exceptionChainContains(e, needle), "Unexpected failure: " + e);
        }
        return e;
    }

    @Test
    void sqlInsertRoundTripsThroughClickHouse() throws Exception {
        String table = "table_api_events";
        createTable(table,
                "id Int64, name String, amount Decimal(18, 4), created_at DateTime64(3), uid UUID, "
                        + "event_day Date, is_active Bool, score Float64, tags Array(String), "
                        + "props Map(String, String), category LowCardinality(String), code FixedString(4)");

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

    /**
     * One column per writable scalar type, two rows holding each column's bounds: the converter,
     * the writer's wire encoding and the server must agree at the extremes. Values are read back
     * as ClickHouse prints them, so the expectations are the server's own rendering.
     */
    @Test
    void everyScalarColumnTypeRoundTripsAtItsBounds() throws Exception {
        String[][] columns = {
                // ClickHouse type   Flink type         low (Flink SQL)                                                       printed                                    high (Flink SQL)                                                     printed
                {"Bool",             "BOOLEAN",         "false",                                                              "false",                                   "true",                                                              "true"},
                {"Int8",             "TINYINT",         "CAST(-128 AS TINYINT)",                                              "-128",                                    "CAST(127 AS TINYINT)",                                              "127"},
                {"Int16",            "SMALLINT",        "CAST(-32768 AS SMALLINT)",                                           "-32768",                                  "CAST(32767 AS SMALLINT)",                                           "32767"},
                {"Int32",            "INT",             "CAST(-2147483648 AS INT)",                                           "-2147483648",                             "CAST(2147483647 AS INT)",                                           "2147483647"},
                {"Int64",            "BIGINT",          "CAST('-9223372036854775808' AS BIGINT)",                             "-9223372036854775808",                    "CAST('9223372036854775807' AS BIGINT)",                             "9223372036854775807"},
                {"Int128",           "DECIMAL(38, 0)",  "CAST('-99999999999999999999999999999999999999' AS DECIMAL(38, 0))", "-99999999999999999999999999999999999999", "CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))", "99999999999999999999999999999999999999"},
                {"Int256",           "DECIMAL(38, 0)",  "CAST('-99999999999999999999999999999999999999' AS DECIMAL(38, 0))", "-99999999999999999999999999999999999999", "CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))", "99999999999999999999999999999999999999"},
                {"UInt8",            "SMALLINT",        "CAST(0 AS SMALLINT)",                                                "0",                                       "CAST(255 AS SMALLINT)",                                             "255"},
                {"UInt16",           "INT",             "0",                                                                  "0",                                       "65535",                                                             "65535"},
                {"UInt32",           "BIGINT",          "CAST(0 AS BIGINT)",                                                  "0",                                       "CAST(4294967295 AS BIGINT)",                                        "4294967295"},
                {"UInt64",           "DECIMAL(20, 0)",  "CAST(0 AS DECIMAL(20, 0))",                                          "0",                                       "CAST('18446744073709551615' AS DECIMAL(20, 0))",                    "18446744073709551615"},
                {"UInt128",          "DECIMAL(38, 0)",  "CAST(0 AS DECIMAL(38, 0))",                                          "0",                                       "CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))",  "99999999999999999999999999999999999999"},
                {"UInt256",          "DECIMAL(38, 0)",  "CAST(0 AS DECIMAL(38, 0))",                                          "0",                                       "CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))",  "99999999999999999999999999999999999999"},
                // Flink folds FLOAT literals to 7 significant digits, so the bound is stated that way (two ULPs below
                // Float32's max); read back widened to Float64, since toString(Float32) rounds to 7 digits as well
                {"Float32",          "FLOAT",           "CAST('-3.402823E38' AS FLOAT)",                                      "-3.4028230607370965e38",                  "CAST('3.402823E38' AS FLOAT)",                                      "3.4028230607370965e38"},
                {"Float64",          "DOUBLE",          "CAST('-1.7976931348623157E308' AS DOUBLE)",                          "-1.7976931348623157e308",                 "CAST('1.7976931348623157E308' AS DOUBLE)",                          "1.7976931348623157e308"},
                {"Decimal(9, 2)",    "DECIMAL(9, 2)",   "CAST('-9999999.99' AS DECIMAL(9, 2))",                               "-9999999.99",                             "CAST('9999999.99' AS DECIMAL(9, 2))",                               "9999999.99"},
                {"Decimal(18, 4)",   "DECIMAL(18, 4)",  "CAST('-99999999999999.9999' AS DECIMAL(18, 4))",                     "-99999999999999.9999",                    "CAST('99999999999999.9999' AS DECIMAL(18, 4))",                     "99999999999999.9999"},
                {"Decimal(38, 10)",  "DECIMAL(38, 10)", "CAST('-9999999999999999999999999999.9999999999' AS DECIMAL(38, 10))", "-9999999999999999999999999999.9999999999", "CAST('9999999999999999999999999999.9999999999' AS DECIMAL(38, 10))", "9999999999999999999999999999.9999999999"},
                {"String",           "STRING",          "''",                                                                 "",                                        "'héllo wörld 日本'",                                                 "héllo wörld 日本"},
                {"FixedString(8)",   "STRING",          "'abcdefgh'",                                                         "abcdefgh",                                "'日本ab'",                                                           "日本ab"},
                {"UUID",             "STRING",          "'00000000-0000-0000-0000-000000000000'",                             "00000000-0000-0000-0000-000000000000",    "'ffffffff-ffff-ffff-ffff-ffffffffffff'",                            "ffffffff-ffff-ffff-ffff-ffffffffffff"},
                {"Date",             "DATE",            "DATE '1970-01-01'",                                                  "1970-01-01",                              "DATE '2149-06-06'",                                                 "2149-06-06"},
                {"Date32",           "DATE",            "DATE '1900-01-01'",                                                  "1900-01-01",                              "DATE '2299-12-31'",                                                 "2299-12-31"},
                {"DateTime",         "TIMESTAMP(0)",    "TIMESTAMP '1970-01-01 00:00:00'",                                    "1970-01-01 00:00:00",                     "TIMESTAMP '2106-02-07 06:28:15'",                                   "2106-02-07 06:28:15"},
                {"DateTime64(3)",    "TIMESTAMP(3)",    "TIMESTAMP '1900-01-01 00:00:00.000'",                                "1900-01-01 00:00:00.000",                 "TIMESTAMP '2299-12-31 23:59:59.999'",                               "2299-12-31 23:59:59.999"},
                // the connector caps DateTime64(9) where the last whole second's ticks still fit Int64
                {"DateTime64(9)",    "TIMESTAMP(9)",    "TIMESTAMP '1900-01-01 00:00:00.000000000'",                          "1900-01-01 00:00:00.000000000",           "TIMESTAMP '2262-04-11 23:47:15.999999999'",                         "2262-04-11 23:47:15.999999999"},
        };
        roundTripAtBounds("table_api_bounds", "ch_bounds", columns);
    }

    /**
     * Every pair whose Flink type is not the column's own counterpart, two rows at the bounds the pair
     * admits: signed widening, signed narrowing and unsigned targets (range-checked), Decimal rescaling
     * and each Decimal spelling, DECIMAL(p, 0) into integers, FLOAT into Float64, CHAR/VARCHAR sources
     * and timestamp precision widening for wall clocks and instants.
     */
    @Test
    void everyConvertingPairRoundTripsAtItsBounds() throws Exception {
        String[][] columns = {
                // ClickHouse type   Flink type          low (Flink SQL)                                                       printed                                     high (Flink SQL)                                                     printed
                // signed widening: the value takes the wider column's Java type on the wire
                {"Int16",            "TINYINT",          "CAST(-128 AS TINYINT)",                                              "-128",                                     "CAST(127 AS TINYINT)",                                              "127"},
                {"Int32",            "SMALLINT",         "CAST(-32768 AS SMALLINT)",                                           "-32768",                                   "CAST(32767 AS SMALLINT)",                                           "32767"},
                {"Int64",            "INT",              "CAST(-2147483648 AS INT)",                                           "-2147483648",                              "CAST(2147483647 AS INT)",                                           "2147483647"},
                {"Int128",           "BIGINT",           "CAST('-9223372036854775808' AS BIGINT)",                             "-9223372036854775808",                     "CAST('9223372036854775807' AS BIGINT)",                             "9223372036854775807"},
                {"Int256",           "TINYINT",          "CAST(-128 AS TINYINT)",                                              "-128",                                     "CAST(127 AS TINYINT)",                                              "127"},
                // signed narrowing: range-checked per record, here at the column's bounds
                {"Int8",             "SMALLINT",         "CAST(-128 AS SMALLINT)",                                             "-128",                                     "CAST(127 AS SMALLINT)",                                             "127"},
                {"Int8",             "BIGINT",           "CAST(-128 AS BIGINT)",                                               "-128",                                     "CAST(127 AS BIGINT)",                                               "127"},
                {"Int16",            "INT",              "CAST(-32768 AS INT)",                                                "-32768",                                   "CAST(32767 AS INT)",                                                "32767"},
                {"Int32",            "BIGINT",           "CAST(-2147483648 AS BIGINT)",                                        "-2147483648",                              "CAST(2147483647 AS BIGINT)",                                        "2147483647"},
                // unsigned targets: range-checked per record, up to whichever of the two ranges ends first
                {"UInt8",            "TINYINT",          "CAST(0 AS TINYINT)",                                                 "0",                                        "CAST(127 AS TINYINT)",                                              "127"},
                {"UInt8",            "BIGINT",           "CAST(0 AS BIGINT)",                                                  "0",                                        "CAST(255 AS BIGINT)",                                               "255"},
                {"UInt16",           "SMALLINT",         "CAST(0 AS SMALLINT)",                                                "0",                                        "CAST(32767 AS SMALLINT)",                                           "32767"},
                {"UInt16",           "BIGINT",           "CAST(0 AS BIGINT)",                                                  "0",                                        "CAST(65535 AS BIGINT)",                                             "65535"},
                {"UInt32",           "INT",              "CAST(0 AS INT)",                                                     "0",                                        "CAST(2147483647 AS INT)",                                           "2147483647"},
                {"UInt64",           "BIGINT",           "CAST(0 AS BIGINT)",                                                  "0",                                        "CAST('9223372036854775807' AS BIGINT)",                             "9223372036854775807"},
                {"UInt128",          "BIGINT",           "CAST(0 AS BIGINT)",                                                  "0",                                        "CAST('9223372036854775807' AS BIGINT)",                             "9223372036854775807"},
                {"UInt256",          "TINYINT",          "CAST(0 AS TINYINT)",                                                 "0",                                        "CAST(127 AS TINYINT)",                                              "127"},
                // Decimal rescaling and each Decimal spelling; the server prints no trailing zeros
                {"Decimal(18, 4)",   "DECIMAL(9, 2)",    "CAST('-9999999.99' AS DECIMAL(9, 2))",                               "-9999999.99",                              "CAST('9999999.99' AS DECIMAL(9, 2))",                               "9999999.99"},
                {"Decimal(38, 10)",  "DECIMAL(9, 0)",    "CAST(-999999999 AS DECIMAL(9, 0))",                                  "-999999999",                               "CAST(999999999 AS DECIMAL(9, 0))",                                  "999999999"},
                {"Decimal32(2)",     "DECIMAL(9, 2)",    "CAST('-9999999.99' AS DECIMAL(9, 2))",                               "-9999999.99",                              "CAST('9999999.99' AS DECIMAL(9, 2))",                               "9999999.99"},
                {"Decimal64(4)",     "DECIMAL(18, 4)",   "CAST('-99999999999999.9999' AS DECIMAL(18, 4))",                     "-99999999999999.9999",                     "CAST('99999999999999.9999' AS DECIMAL(18, 4))",                     "99999999999999.9999"},
                {"Decimal128(10)",   "DECIMAL(38, 10)",  "CAST('-9999999999999999999999999999.9999999999' AS DECIMAL(38, 10))", "-9999999999999999999999999999.9999999999", "CAST('9999999999999999999999999999.9999999999' AS DECIMAL(38, 10))", "9999999999999999999999999999.9999999999"},
                {"Decimal256(20)",   "DECIMAL(38, 10)",  "CAST('-9999999999999999999999999999.9999999999' AS DECIMAL(38, 10))", "-9999999999999999999999999999.9999999999", "CAST('9999999999999999999999999999.9999999999' AS DECIMAL(38, 10))", "9999999999999999999999999999.9999999999"},
                // DECIMAL(p, 0) into integers: below the digit boundary unchecked, at it or into unsigned range-checked
                {"Int8",             "DECIMAL(3, 0)",    "CAST(-128 AS DECIMAL(3, 0))",                                        "-128",                                     "CAST(127 AS DECIMAL(3, 0))",                                        "127"},
                {"Int16",            "DECIMAL(5, 0)",    "CAST(-32768 AS DECIMAL(5, 0))",                                      "-32768",                                   "CAST(32767 AS DECIMAL(5, 0))",                                      "32767"},
                {"Int32",            "DECIMAL(9, 0)",    "CAST(-999999999 AS DECIMAL(9, 0))",                                  "-999999999",                               "CAST(999999999 AS DECIMAL(9, 0))",                                  "999999999"},
                {"Int64",            "DECIMAL(19, 0)",   "CAST('-9223372036854775808' AS DECIMAL(19, 0))",                     "-9223372036854775808",                     "CAST('9223372036854775807' AS DECIMAL(19, 0))",                     "9223372036854775807"},
                {"UInt32",           "DECIMAL(10, 0)",   "CAST(0 AS DECIMAL(10, 0))",                                          "0",                                        "CAST(4294967295 AS DECIMAL(10, 0))",                                "4294967295"},
                // FLOAT widens exactly
                {"Float64",          "FLOAT",            "CAST('-3.402823E38' AS FLOAT)",                                      "-3.4028230607370965e38",                   "CAST('3.402823E38' AS FLOAT)",                                      "3.4028230607370965e38"},
                // CHAR/VARCHAR sources; a short value is zero-padded into FixedString, UUID text may be upper case
                {"String",           "CHAR(4)",          "CAST('AB12' AS CHAR(4))",                                            "AB12",                                     "CAST('ZZ99' AS CHAR(4))",                                           "ZZ99"},
                {"FixedString(4)",   "CHAR(4)",          "CAST('AB12' AS CHAR(4))",                                            "AB12",                                     "CAST('ZZ99' AS CHAR(4))",                                           "ZZ99"},
                {"FixedString(4)",   "VARCHAR(4)",       "CAST('ab' AS VARCHAR(4))",                                           "ab",                                       "CAST('ZZ99' AS VARCHAR(4))",                                        "ZZ99"},
                {"UUID",             "VARCHAR(36)",      "'123E4567-E89B-12D3-A456-426614174000'",                             "123e4567-e89b-12d3-a456-426614174000",     "'ffffffff-ffff-ffff-ffff-ffffffffffff'",                            "ffffffff-ffff-ffff-ffff-ffffffffffff"},
                // timestamp precision widening, for wall clocks and for instants
                {"DateTime64(3)",    "TIMESTAMP(0)",     "TIMESTAMP '1970-01-01 00:00:00'",                                    "1970-01-01 00:00:00.000",                  "TIMESTAMP '2299-12-31 23:59:59'",                                   "2299-12-31 23:59:59.000"},
                {"DateTime64(9)",    "TIMESTAMP(3)",     "TIMESTAMP '1900-01-01 00:00:00.000'",                                "1900-01-01 00:00:00.000000000",            "TIMESTAMP '2026-01-02 03:04:05.678'",                               "2026-01-02 03:04:05.678000000"},
                {"DateTime",         "TIMESTAMP_LTZ(0)", "CAST(TO_TIMESTAMP_LTZ(0, 3) AS TIMESTAMP_LTZ(0))",                   "1970-01-01 00:00:00",                      "CAST(TO_TIMESTAMP_LTZ(4294967295000, 3) AS TIMESTAMP_LTZ(0))",      "2106-02-07 06:28:15"},
                {"DateTime64(9)",    "TIMESTAMP_LTZ(3)", "TO_TIMESTAMP_LTZ(" + epochMillis("1900-01-01T00:00:00Z") + ", 3)",   "1900-01-01 00:00:00.000000000",            "TO_TIMESTAMP_LTZ(" + epochMillis("2026-01-02T03:04:05.678Z") + ", 3)", "2026-01-02 03:04:05.678000000"},
        };
        roundTripAtBounds("table_api_conversions", "ch_conversions", columns);
    }

    /** Writes each column's two values through the sink and asserts the server prints them back as given. */
    private static void roundTripAtBounds(String table, String flinkTable, String[][] columns) throws Exception {
        StringBuilder clickHouseColumns = new StringBuilder("id Int64");
        StringBuilder flinkColumns = new StringBuilder("id BIGINT NOT NULL");
        StringBuilder lows = new StringBuilder("(1");
        StringBuilder highs = new StringBuilder("(2");
        StringBuilder readColumns = new StringBuilder("id");
        for (int i = 0; i < columns.length; i++) {
            clickHouseColumns.append(", c").append(i).append(' ').append(columns[i][0]);
            flinkColumns.append(", c").append(i).append(' ').append(columns[i][1]).append(" NOT NULL");
            lows.append(", ").append(columns[i][2]);
            highs.append(", ").append(columns[i][4]);
            readColumns.append(", ").append(printed(columns[i][0], "c" + i)).append(" AS s").append(i);
        }
        createTable(table, clickHouseColumns.toString());

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl(flinkTable, table, flinkColumns.toString()));
        env.executeSql("INSERT INTO " + flinkTable + " VALUES " + lows + "), " + highs + ")").await();

        List<GenericRecord> rows = readBack(readColumns.toString(), table, "id", 2);
        Assertions.assertEquals(2, rows.size());
        for (int i = 0; i < columns.length; i++) {
            Assertions.assertEquals(columns[i][3], rows.get(0).getString("s" + i), columns[i][0] + " low");
            Assertions.assertEquals(columns[i][5], rows.get(1).getString("s" + i), columns[i][0] + " high");
        }
    }

    /** Float32 is widened so toString does not round it to 7 digits; FixedString drops its zero padding. */
    private static String printed(String clickHouseType, String column) {
        if (clickHouseType.equals("Float32")) {
            return "toString(toFloat64(" + column + "))";
        }
        if (clickHouseType.startsWith("FixedString")) {
            return "replaceAll(toString(" + column + "), '\\x00', '')";
        }
        return "toString(" + column + ")";
    }

    private static long epochMillis(String instant) {
        return Instant.parse(instant).toEpochMilli();
    }

    /** The same conversions inside composites, plus integer and FixedString map keys restored from their text (24.3 has no Decimal keys). */
    @Test
    void conversionsHoldInsideCompositesAndTypedMapKeysRoundTrip() throws Exception {
        String table = "table_api_nested_conversions";
        createTable(table,
                "id Int64, wide Array(Int128), unsigned Array(UInt64), floats Array(Float64), "
                        + "money Array(Decimal(18, 4)), days Array(Date32), narrow Tuple(Int8, UInt16), "
                        + "stamped Tuple(DateTime64(3), UUID), maybe Array(Nullable(UInt8)), "
                        + "by_int Map(Int32, String), by_big Map(Int128, UInt8), by_fixed Map(FixedString(2), Int32)");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_nested_conversions", table,
                "id BIGINT NOT NULL,"
                        + "wide ARRAY<BIGINT NOT NULL> NOT NULL,"
                        + "unsigned ARRAY<BIGINT NOT NULL> NOT NULL,"
                        + "floats ARRAY<FLOAT NOT NULL> NOT NULL,"
                        + "money ARRAY<DECIMAL(9, 2) NOT NULL> NOT NULL,"
                        + "days ARRAY<DATE NOT NULL> NOT NULL,"
                        + "narrow ROW<a SMALLINT NOT NULL, b INT NOT NULL> NOT NULL,"
                        + "stamped ROW<stamp TIMESTAMP(3) NOT NULL, uid STRING NOT NULL> NOT NULL,"
                        + "maybe ARRAY<SMALLINT> NOT NULL,"
                        + "by_int MAP<INT, STRING NOT NULL> NOT NULL,"
                        + "by_big MAP<DECIMAL(38, 0), SMALLINT NOT NULL> NOT NULL,"
                        + "by_fixed MAP<STRING, INT NOT NULL> NOT NULL"));
        env.executeSql("INSERT INTO ch_nested_conversions VALUES (1, "
                + "ARRAY[CAST('-9223372036854775808' AS BIGINT), CAST('9223372036854775807' AS BIGINT)], "
                + "ARRAY[CAST(0 AS BIGINT), CAST('9223372036854775807' AS BIGINT)], "
                + "ARRAY[CAST(1.5 AS FLOAT)], "
                + "ARRAY[CAST('12.5' AS DECIMAL(9, 2))], "
                + "ARRAY[DATE '1900-01-01', DATE '2299-12-31'], "
                + "ROW(CAST(-128 AS SMALLINT), 65535), "
                + "ROW(TIMESTAMP '2026-01-02 03:04:05.678', '123e4567-e89b-12d3-a456-426614174000'), "
                + "ARRAY[CAST(NULL AS SMALLINT), CAST(255 AS SMALLINT)], "
                + "MAP[-7, 'neg', 2147483647, 'max'], "
                + "MAP[CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0)), CAST(255 AS SMALLINT)], "
                + "MAP['ab', 1])").await();

        List<GenericRecord> rows = readBack(
                "id, toString(wide) AS wide_s, toString(unsigned) AS unsigned_s, toString(floats) AS floats_s, "
                        + "toString(money) AS money_s, toString(days) AS days_s, toString(narrow) AS narrow_s, "
                        + "toString(stamped) AS stamped_s, toString(maybe) AS maybe_s, "
                        + "by_int[-7] AS neg, by_int[2147483647] AS max, "
                        + "toString(by_big) AS by_big_s, toString(by_fixed) AS by_fixed_s",
                table, "id", 1);
        Assertions.assertEquals(1, rows.size());
        GenericRecord row = rows.get(0);
        Assertions.assertEquals("[-9223372036854775808,9223372036854775807]", row.getString("wide_s"));
        Assertions.assertEquals("[0,9223372036854775807]", row.getString("unsigned_s"));
        Assertions.assertEquals("[1.5]", row.getString("floats_s"));
        Assertions.assertEquals("[12.5]", row.getString("money_s"));
        Assertions.assertEquals("['1900-01-01','2299-12-31']", row.getString("days_s"));
        Assertions.assertEquals("(-128,65535)", row.getString("narrow_s"));
        Assertions.assertEquals("('2026-01-02 03:04:05.678','123e4567-e89b-12d3-a456-426614174000')",
                row.getString("stamped_s"));
        Assertions.assertEquals("[NULL,255]", row.getString("maybe_s"));
        Assertions.assertEquals("neg", row.getString("neg"));
        Assertions.assertEquals("max", row.getString("max"));
        Assertions.assertEquals("{99999999999999999999999999999999999999:255}", row.getString("by_big_s"));
        Assertions.assertEquals("{'ab':1}", row.getString("by_fixed_s"));
    }

    @Test
    void replanningAfterAlterSeesTheCurrentSchema() throws Exception {
        String table = "table_api_alter";
        createTable(table, "id Int64");

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
    void unreachableServerFailsPlanningFastWhateverSinkMaxRetries() {
        TableEnvironment env = tableEnvironment();
        // Port 1 never listens, so the planning DESCRIBE fails at once with connection refused.
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
        assertFailsWith(() -> env.executeSql("INSERT INTO ch_unreachable VALUES (1)"),
                "Could not read the schema", "Connection refused");
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        // sink.max-retries governs batch retries only; 100 000 attempts here would block for days.
        Assertions.assertTrue(elapsedMs < 60_000,
                "Planning blocked for " + elapsedMs + "ms — is sink.max-retries driving it?");
    }

    @Test
    void negativeBigIntIntoUInt32FailsNamingTheColumn() throws Exception {
        String table = "table_api_unsigned";
        createTable(table, "id Int64, hits UInt32");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_unsigned", table, "id BIGINT NOT NULL, hits BIGINT NOT NULL"));

        assertFailsWith(() -> env.executeSql("INSERT INTO ch_unsigned VALUES (1, -1)").await(),
                "Column 'hits': value -1 is outside the UInt32 range");
    }

    @Test
    void strictNumericMappingRejectsARangeCheckedPairAtPlanning() throws Exception {
        String table = "table_api_strict";
        createTable(table, "id Int64, hits UInt32");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_strict", table, "id BIGINT NOT NULL, hits BIGINT NOT NULL",
                ", 'sink.strict-numeric-mapping' = 'true'"));

        assertFailsWith(() -> env.executeSql("INSERT INTO ch_strict VALUES (1, 1)"),
                "Column 'hits'", "UInt32 range", "'sink.strict-numeric-mapping'");
    }

    @Test
    void outOfRangeDate32FailsNamingTheColumn() throws Exception {
        String table = "table_api_date32";
        createTable(table, "id Int64, event_day Date32");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_date32", table, "id BIGINT NOT NULL, event_day DATE NOT NULL"));

        // Pre-fix this was written raw and stored as a different date, with no error anywhere.
        assertFailsWith(() -> env.executeSql("INSERT INTO ch_date32 VALUES (1, DATE '9999-12-31')").await(),
                "Column 'event_day': DATE value 9999-12-31 is outside the ClickHouse Date32 range");
    }

    @Test
    void unknownFlinkColumnFailsAtPlanningWithPreciseMessage() throws Exception {
        String table = "table_api_reject";
        createTable(table, "id Int64");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_reject", table,
                "id BIGINT NOT NULL, nickname STRING NOT NULL"));

        assertFailsWith(() -> env.executeSql("INSERT INTO ch_reject VALUES (1, 'nick')"),
                "Column 'nickname' declared in the Flink schema does not exist in");
    }

    @Test
    void nullValuesRoundTripIntoNullableColumns() throws Exception {
        String table = "table_api_nullable";
        createTable(table, "id Int64, name Nullable(String), score Nullable(Float64), event_day Nullable(Date)");

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
        createTable(table, "id Int64, tags Map(String, UInt64)");

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
        createTable(table,
                "id Int64, pair Tuple(Int32, String), nums Array(Nullable(Int32)), "
                        + "pairs Array(Tuple(Int32, String)), nested Tuple(Int32, Tuple(Int32, String))");

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
        createTable(table,
                "id Int64, pair Tuple(a Int32, b String), pairs Array(Tuple(a Int32, b String)), "
                        + "by_key Map(String, Tuple(a Int32, b String))");

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
        createTable(table, "id Int64, pair Tuple(Int32, String), nums Array(Int32)");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_nested_null", table,
                "id BIGINT NOT NULL,"
                        + "pair ROW<a INT NOT NULL, b STRING NOT NULL> NOT NULL,"
                        + "nums ARRAY<INT NOT NULL> NOT NULL"));

        assertFailsWith(() -> env.executeSql(
                "INSERT INTO ch_nested_null VALUES (1, ROW(CAST(NULL AS INT), 'x'), ARRAY[1])").await(),
                "Column 'pair': null ROW field 1");
        assertFailsWith(() -> env.executeSql(
                "INSERT INTO ch_nested_null VALUES (2, ROW(7, 'x'), ARRAY[1, CAST(NULL AS INT)])").await(),
                "Column 'nums': null array element 2");
        Assertions.assertEquals(0, readBack("id", table, "id", 0).size());
    }

    @Test
    void simpleAggregateFunctionColumnAcceptsItsInnerType() throws Exception {
        String table = "table_api_simple_agg";
        createTable(table, "id Int64, total SimpleAggregateFunction(sum, Int64)", "AggregatingMergeTree() ORDER BY id");

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
        createTable(table, "name String, cnt Int64", "MergeTree() ORDER BY name");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_insert_only", table, "name STRING NOT NULL, cnt BIGINT NOT NULL"));

        // A streaming GROUP BY emits updates; the insert-only sink must reject the plan (#148).
        assertFailsWith(() -> env.executeSql("INSERT INTO ch_insert_only "
                        + "SELECT name, COUNT(*) FROM (VALUES ('a'), ('a'), ('b')) AS t(name) "
                        + "GROUP BY name"),
                "doesn't support consuming update changes");
    }

    @Test
    void omittedClickHouseColumnsWithDefaultsAreBackfilled() throws Exception {
        String table = "table_api_defaults";
        createTable(table, "id Int64, note Nullable(String), tag String DEFAULT 'none'");

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
        createTable(table, "id Int64, req String, tags Array(String)");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_no_default", table, "id BIGINT NOT NULL"));
        env.executeSql("INSERT INTO ch_no_default VALUES (1)").await();

        List<GenericRecord> rows = readBack("id, req, length(tags) AS tags_len", table, "id", 1);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(1L, rows.get(0).getLong("id"));
        Assertions.assertEquals("", rows.get(0).getString("req"));
        Assertions.assertEquals(0L, rows.get(0).getLong("tags_len"));
    }

    /**
     * A column list the statement does not fill must reach the server as a narrower header, not as
     * the planner's padding nulls — those would overwrite the columns' DEFAULTs.
     */
    @Test
    void partialInsertLeavesTheOmittedColumnsToTheServerDefault() throws Exception {
        assumeTargetColumnsAreReported();
        String table = "table_api_partial_insert";
        createTable(table, "id Int64, note Nullable(String) DEFAULT 'unset', n Nullable(Int32) DEFAULT 42");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_partial", table, "id BIGINT NOT NULL, note STRING, n INT"));
        env.executeSql("INSERT INTO ch_partial (id) VALUES (1), (2)").await();

        List<GenericRecord> rows = readBack(
                "id, ifNull(note, '<null>') AS note_s, ifNull(n, -1) AS n_v", table, "id", 2);
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals("unset", rows.get(0).getString("note_s"));
        Assertions.assertEquals(42, rows.get(0).getInteger("n_v"));
        Assertions.assertEquals("unset", rows.get(1).getString("note_s"));
        Assertions.assertEquals(42, rows.get(1).getInteger("n_v"));
    }

    /** The other half of the contract: without a column list a null is the user's value, not an omission. */
    @Test
    void insertWithoutAColumnListStillWritesAnExplicitNullOverTheDefault() throws Exception {
        String table = "table_api_explicit_null";
        createTable(table, "id Int64, note Nullable(String) DEFAULT 'unset'");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_explicit_null", table, "id BIGINT NOT NULL, note STRING"));
        env.executeSql("INSERT INTO ch_explicit_null VALUES (1, CAST(NULL AS STRING))").await();

        List<GenericRecord> rows = readBack("id, ifNull(note, '<null>') AS note_s", table, "id", 1);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("<null>", rows.get(0).getString("note_s"));
    }

    /**
     * Pins what the column list's indices count: the declared columns, a computed one included.
     * Counting physical columns instead would shift 'tag' onto 'note' here.
     */
    @Test
    void partialInsertResolvesIndicesPastAComputedColumn() throws Exception {
        assumeTargetColumnsAreReported();
        String table = "table_api_partial_computed";
        createTable(table, "id Int64, note Nullable(String) DEFAULT 'note-default', "
                + "tag Nullable(String) DEFAULT 'tag-default'");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_partial_computed", table,
                "id BIGINT NOT NULL, doubled AS id * 2, note STRING, tag STRING"));
        env.executeSql("INSERT INTO ch_partial_computed (id, tag) VALUES (1, 'set')").await();

        List<GenericRecord> rows = readBack("id, note, tag", table, "id", 1);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("note-default", rows.get(0).getString("note"));
        Assertions.assertEquals("set", rows.get(0).getString("tag"));
    }

    /**
     * A MATERIALIZED column may be declared as long as the statement leaves it to the server:
     * planning cannot reject it, because only the INSERT's column list decides whether the sink
     * would write it. Rejecting at resolution would make the column list unusable here.
     *
     * <p>The Flink column has to be nullable — the planner refuses to leave a NOT NULL column out
     * of a column list. Its nullability never reaches the type matrix, because the column is
     * skipped before it.
     */
    @Test
    void partialInsertOmittingAMaterializedColumnLeavesItToTheServer() throws Exception {
        assumeTargetColumnsAreReported();
        String table = "table_api_partial_materialized";
        createTable(table, "id Int64, mat Int64 MATERIALIZED id * 2");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_partial_mat", table, "id BIGINT NOT NULL, mat BIGINT"));
        env.executeSql("INSERT INTO ch_partial_mat (id) VALUES (1), (2)").await();

        List<GenericRecord> rows = readBack("id, mat", table, "id", 2);
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals(2L, rows.get(0).getLong("mat"));
        Assertions.assertEquals(4L, rows.get(1).getLong("mat"));
    }

    /**
     * The other half of that contract: a column list that names the column is still an error, and
     * the hint depends on whether a column list can reach the sink at all.
     */
    @Test
    void anInsertColumnListNamingAMaterializedColumnIsRejected() throws Exception {
        String table = "table_api_materialized_targeted";
        createTable(table, "id Int64, mat Int64 MATERIALIZED id * 2");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_mat_targeted", table, "id BIGINT NOT NULL, mat BIGINT"));

        assertFailsWith(() -> env.executeSql("INSERT INTO ch_mat_targeted (id, mat) VALUES (1, 9)"),
                "is MATERIALIZED", "the server computes it",
                TargetColumns.isSupported()
                        ? "Drop the column from the INSERT column list."
                        : "Exclude the column from the Flink schema.");
    }

    /** EPHEMERAL is the same story: legal to declare, as long as the statement omits it. */
    @Test
    void partialInsertOmittingAnEphemeralColumnLeavesItToTheServer() throws Exception {
        assumeTargetColumnsAreReported();
        String table = "table_api_partial_ephemeral";
        createTable(table, "id Int64, payload String EPHEMERAL, norm String DEFAULT upper(payload)");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_partial_ephemeral", table, "id BIGINT NOT NULL, payload STRING"));
        env.executeSql("INSERT INTO ch_partial_ephemeral (id) VALUES (1)").await();

        // 'norm' is omitted too, so the server evaluates its DEFAULT over the ephemeral default ''.
        List<GenericRecord> rows = readBack("id, norm", table, "id", 1);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("", rows.get(0).getString("norm"));
    }

    private static void assumeTargetColumnsAreReported() {
        Assumptions.assumeTrue(TargetColumns.isSupported(),
                "Flink 1.17 does not report the INSERT column list to the sink, so a partial "
                + "insert there still writes the planner's padding nulls");
    }

    @Test
    void statementSetInsertsIntoTheSameSinkTwice() throws Exception {
        String table = "table_api_stmt_set";
        createTable(table, "id Int64, src String");

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
        createTable(table, "a Int64, b String, c Float64", "MergeTree() ORDER BY a");

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
        createTable(table, "id Int64, label Int64");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_mismatch", table, "id BIGINT NOT NULL, label STRING NOT NULL"));

        assertFailsWith(() -> env.executeSql("INSERT INTO ch_mismatch VALUES (1, 'nope')"),
                "Column 'label': Flink type STRING NOT NULL cannot be written to ClickHouse column 'label Int64'");
    }

    @Test
    void ignoredUnknownFlinkColumnIsSkippedAtWriteTime() throws Exception {
        String table = "table_api_ignore_unknown";
        createTable(table, "id Int64, name String");

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
        createTable(table, "id Int64, name String");

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
        createTable(table, "id Int64, v String");

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
        createTable(table, "id Int64, ts DateTime64(3)");

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
        createTable(table, "id Int64, ts DateTime64(3)");

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
            createTable(table, "id Int64, j JSON");
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
        createTable("table-api-canary", "id Int64");
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
        createTable(table, "id Int64");

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

        assertFailsWith(() -> env.executeSql("INSERT INTO ch_bare_prefix VALUES (1)"),
                "Option 'clickhouse.server.' has no key after the prefix");
    }

    @Test
    void zeroScaleDecimalsWriteIntoTheIntegersTheirDigitsCover() throws Exception {
        String table = "table_api_decimal_ints";
        createTable(table, "id Int64, small UInt8, big Int64");

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
    void doubleIntoFloat32IsRejectedAtPlanning() throws Exception {
        String table = "table_api_double_float32";
        createTable(table, "id Int64, ratio Float32");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_double_float32", table, "id BIGINT NOT NULL, ratio DOUBLE NOT NULL"));

        // Rounding a double to single precision changes the value, so the pair is refused in every mode.
        assertFailsWith(() -> env.executeSql("INSERT INTO ch_double_float32 VALUES (1, 0.1)"),
                "Column 'ratio'", "Float32 would round DOUBLE values", "CAST the value to FLOAT");
    }

    @Test
    void overlongStringIntoFixedStringFailsNamingTheColumn() throws Exception {
        String table = "table_api_fixed_string";
        createTable(table, "id Int64, code FixedString(4)");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_fixed_string", table, "id BIGINT NOT NULL, code STRING NOT NULL"));

        assertFailsWith(() -> env.executeSql("INSERT INTO ch_fixed_string VALUES (1, 'ABCDE')").await(),
                "Column 'code': value of 5 bytes does not fit FixedString(4)");
        Assertions.assertEquals(0, readBack("id", table, "id", 0).size());
    }

    /** The option gates numeric range checks only; date, timestamp, UUID and FixedString values are still checked per record. */
    @Test
    void strictNumericMappingLeavesValueCheckedPairsAlone() throws Exception {
        String table = "table_api_strict_values";
        createTable(table, "id Int64, event_day Date, ts DateTime64(3), uid UUID, code FixedString(4)");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_strict_values", table,
                "id BIGINT NOT NULL, event_day DATE NOT NULL, ts TIMESTAMP(3) NOT NULL, "
                        + "uid STRING NOT NULL, code STRING NOT NULL",
                ", 'sink.strict-numeric-mapping' = 'true'"));
        env.executeSql("INSERT INTO ch_strict_values VALUES (1, DATE '2026-01-02', TIMESTAMP '2026-01-02 03:04:05.678', "
                + "'123e4567-e89b-12d3-a456-426614174000', 'AB12')").await();

        List<GenericRecord> rows = readBack(
                "id, toString(event_day) AS day_s, toString(ts) AS ts_s, toString(uid) AS uid_s, toString(code) AS code_s",
                table, "id", 1);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("2026-01-02", rows.get(0).getString("day_s"));
        Assertions.assertEquals("2026-01-02 03:04:05.678", rows.get(0).getString("ts_s"));
        Assertions.assertEquals("123e4567-e89b-12d3-a456-426614174000", rows.get(0).getString("uid_s"));
        Assertions.assertEquals("AB12", rows.get(0).getString("code_s"));

        assertFailsWith(() -> env.executeSql("INSERT INTO ch_strict_values VALUES (2, DATE '2026-01-02', "
                        + "TIMESTAMP '2026-01-02 03:04:05.678', 'not-a-uuid', 'AB12')").await(),
                "Column 'uid': value is not a valid UUID: not-a-uuid");
    }

    @Test
    void nullableUnsignedColumnsAreRejectedAtPlanning() throws Exception {
        String table = "table_api_nullable_unsigned";
        createTable(table, "id Int64, hits Nullable(UInt32)");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_nullable_unsigned", table, "id BIGINT NOT NULL, hits BIGINT"));

        // DataWriter writes a null into Nullable(UInt*) as 0 (issue #144), so planning refuses the pair.
        assertFailsWith(() -> env.executeSql("INSERT INTO ch_nullable_unsigned VALUES (1, 1)"),
                "Column 'hits'", "issue #144");
    }

    @Test
    void defaultTimestampPrecisionIsRejectedForDateTime64Of3() throws Exception {
        String table = "table_api_ts_precision";
        createTable(table, "id Int64, ts DateTime64(3)");

        TableEnvironment env = tableEnvironment();
        // Flink's TIMESTAMP defaults to precision 6, which DateTime64(3) would truncate.
        env.executeSql(sinkDdl("ch_ts_precision", table, "id BIGINT NOT NULL, ts TIMESTAMP NOT NULL"));

        assertFailsWith(() -> env.executeSql("INSERT INTO ch_ts_precision VALUES (1, TIMESTAMP '2026-01-02 03:04:05')"),
                "Column 'ts'", "precision 6 exceeds the column's scale 3");
    }

    @Test
    void sinkParallelismSplitsTheInsertAcrossThatManyWriters() throws Exception {
        String table = "table_api_parallelism";
        createTable(table, "id Int64");

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
        createTable(table, "id Int64");

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
        createTable(table, "id Int64");

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
    void twoEntryBufferBackpressuresWithoutLosingRows() throws Exception {
        String table = "table_api_tiny_buffer";
        createTable(table, "id Int64");

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
        createTable(table, "id Int64, payload String");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_record_bytes", table, "id BIGINT NOT NULL, payload STRING NOT NULL",
                ", 'sink.record.max-bytes' = '64b'"));
        // A 13-byte RowBinary row fits; a 200-char payload is rejected by the AsyncSink writer.
        env.executeSql("INSERT INTO ch_record_bytes VALUES (1, 'tiny')").await();
        Assertions.assertEquals(1, readBack("id", table, "id", 1).size());

        assertFailsWith(() -> env.executeSql("INSERT INTO ch_record_bytes VALUES (2, REPEAT('x', 200))").await(),
                "maxRecordSizeInBytes was set to [64]");
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

        assertFailsWith(() -> env.executeSql("INSERT INTO ch_stop_flink VALUES (1, '10'), (2, 'not-a-number')").await(),
                "not-a-number");
    }

    @Test
    void ephemeralColumnsAreRejectedWhenTheStatementWritesThem() throws Exception {
        String table = "table_api_ephemeral";
        createTable(table, "id Int64, payload String EPHEMERAL, norm String DEFAULT lower(payload)");

        TableEnvironment env = tableEnvironment();
        // The sink's INSERT carries no column list, so a header naming 'payload' would be dropped silently.
        env.executeSql(sinkDdl("ch_ephemeral", table, "id BIGINT NOT NULL, payload STRING NOT NULL"));

        // No column list: every declared column is written, 'payload' included.
        assertFailsWith(() -> env.executeSql("INSERT INTO ch_ephemeral VALUES (1, 'X')"),
                "is EPHEMERAL", "sends no column list");
    }

    // ------------------------------------------------------------------------------------
    // Rejections: every pair the matrix refuses at planning, and every check it defers to
    // write time. Both are tables so a rule that stops firing shows up as a named row rather
    // than as a silently accepted insert.
    // ------------------------------------------------------------------------------------

    /** Splits the {@code ~}-separated needle cell of the rejection tables below. */
    private static String[] needles(String cell) {
        return cell.split("~");
    }

    /** Fails the row by name rather than by index when a table-driven rejection stops firing. */
    private static void assertRejectedWith(String description, Executable insert, String... needles) {
        Exception failure = Assertions.assertThrows(Exception.class, insert, description + " was accepted");
        for (String needle : needles) {
            Assertions.assertTrue(exceptionChainContains(failure, needle),
                    description + ": no '" + needle + "' in " + failure);
        }
    }

    /** One ClickHouse column {@code c} of the given type, plus {@code id}; table name per row. */
    private static String rejectionTable(String prefix, int index, String clickHouseType) throws Exception {
        String table = String.format("table_api_%s_%02d", prefix, index);
        createTable(table, "id Int64, c " + clickHouseType);
        return table;
    }

    /**
     * Every pair rejected at planning: the scalar matrix cells, the structural composite rules
     * (element/key/value/field nullability, Tuple arity and named-Tuple order, unsupported map
     * keys) and the strict-mapping paths, scalar and nested. The inserted value is always valid —
     * these fail on the types alone, before a record exists.
     */
    @Test
    void everyRejectedPairFailsAtPlanningWithItsReason() throws Exception {
        String strict = ", 'sink.strict-numeric-mapping' = 'true'";
        String[][] pairs = {
            // ClickHouse type | Flink type of column 'c' | a valid value | options | expected message fragments
            // --- ClickHouse targets the sink has no write path for
            {"Enum8('a' = 1, 'b' = 2)", "STRING NOT NULL", "'a'", "",
             "is not yet supported by the sink~issue #43"},
            {"Array(SimpleAggregateFunction(sum, Int64))", "ARRAY<BIGINT NOT NULL> NOT NULL",
             "ARRAY[CAST(1 AS BIGINT)]", "", "SimpleAggregateFunction is only writable as a top-level column"},
            // --- Decimal: the four ways a Decimal source can fail to fit
            {"Decimal(10, 2)", "DECIMAL(10, 4) NOT NULL", "CAST('1.2345' AS DECIMAL(10, 4))", "",
             "Column 'c'~scale 4 exceeds the column's scale 2"},
            {"Decimal(10, 2)", "DECIMAL(12, 2) NOT NULL", "CAST('1.23' AS DECIMAL(12, 2))", "",
             "10 integer digits exceed the column's 8 integer digits"},
            {"Int32", "DECIMAL(9, 2) NOT NULL", "CAST('1.23' AS DECIMAL(9, 2))", "",
             "only DECIMAL(p, 0) can be written to an integer column"},
            {"Int32", "DECIMAL(11, 0) NOT NULL", "CAST(1 AS DECIMAL(11, 0))", "",
             "precision 11 exceeds Int32's 10 digits"},
            // --- no conversion at all between the two type families
            {"Bool", "INT NOT NULL", "CAST(1 AS INT)", "", "no supported conversion~for INT NOT NULL: Int8..Int256, UInt8..UInt256"},
            {"Int64", "BOOLEAN NOT NULL", "TRUE", "", "supported ClickHouse types for BOOLEAN NOT NULL: Bool"},
            {"String", "DATE NOT NULL", "DATE '2026-01-02'", "", "supported ClickHouse types for DATE NOT NULL: Date, Date32"},
            {"Date", "TIMESTAMP(3) NOT NULL", "TIMESTAMP '2026-01-02 03:04:05.678'", "",
             "DateTime, DateTime64(s) with s >= the Flink precision"},
            // --- nullability, at the column and at every nested position
            {"Int64", "BIGINT", "CAST(1 AS BIGINT)", "",
             "Column 'c'~is nullable but ClickHouse column 'c Int64' is not Nullable"},
            {"Array(Int32)", "ARRAY<INT> NOT NULL", "ARRAY[CAST(1 AS INT)]", "",
             "the Flink array element type INT is nullable but the ClickHouse element type Int32 is not Nullable"},
            {"Map(String, Int32)", "MAP<STRING NOT NULL, INT> NOT NULL", "MAP['k', CAST(1 AS INT)]", "",
             "the Flink map value type INT is nullable but the ClickHouse type Int32 is not Nullable"},
            {"Tuple(Int32, String)", "ROW<a INT, b STRING NOT NULL> NOT NULL", "ROW(CAST(1 AS INT), 'x')", "",
             "the Flink ROW field 'a' is nullable but the ClickHouse type Int32 is not Nullable"},
            // --- composite structure
            {"Tuple(Int32, String)", "ROW<a INT NOT NULL> NOT NULL", "ROW(CAST(1 AS INT))", "",
             "ROW has 1 fields but the Tuple has 2 elements"},
            {"Tuple(a Int32, b String)", "ROW<b INT NOT NULL, a STRING NOT NULL> NOT NULL",
             "ROW(CAST(1 AS INT), 'x')", "", "bind to Tuple elements by position, but the Tuple names them"},
            // --- map keys are checkpointed as strings, so only types that parse back are allowed
            {"Map(UInt64, String)", "MAP<DECIMAL(20, 0) NOT NULL, STRING NOT NULL> NOT NULL",
             "MAP[CAST(1 AS DECIMAL(20, 0)), 'v']", "",
             "Map keys of type UInt64 are not supported~use an Int64 or UInt128 key column instead"},
            {"Map(UUID, String)", "MAP<STRING NOT NULL, STRING NOT NULL> NOT NULL", "MAP['k', 'v']", "",
             "Map key type UUID is not supported~cannot be restored from a string"},
            // --- strict numeric mapping, on each path that would otherwise range-check per record
            {"UInt32", "BIGINT NOT NULL", "CAST(1 AS BIGINT)", strict,
             "Column 'c'~UInt32 range~'sink.strict-numeric-mapping'"},
            {"Int32", "DECIMAL(10, 0) NOT NULL", "CAST(1 AS DECIMAL(10, 0))", strict,
             "Column 'c'~Int32 range~'sink.strict-numeric-mapping'"},
            {"Array(UInt64)", "ARRAY<BIGINT NOT NULL> NOT NULL", "ARRAY[CAST(1 AS BIGINT)]", strict,
             "Column 'c'~array element~UInt64 range~'sink.strict-numeric-mapping'"},
            {"Map(String, UInt32)", "MAP<STRING NOT NULL, BIGINT NOT NULL> NOT NULL",
             "MAP['k', CAST(1 AS BIGINT)]", strict, "Column 'c'~map value~UInt32 range~'sink.strict-numeric-mapping'"},
            {"Tuple(a UInt8, b String)", "ROW<a BIGINT NOT NULL, b STRING NOT NULL> NOT NULL",
             "ROW(CAST(1 AS BIGINT), 'x')", strict, "Column 'c'~ROW field 'a'~UInt8 range~'sink.strict-numeric-mapping'"},
        };

        TableEnvironment env = tableEnvironment();
        for (int i = 0; i < pairs.length; i++) {
            String table = rejectionTable("reject", i, pairs[i][0]);
            String flinkTable = "ch_reject_pair_" + i;
            String value = pairs[i][2];
            env.executeSql(sinkDdl(flinkTable, table, "id BIGINT NOT NULL, c " + pairs[i][1], pairs[i][3]));

            String description = pairs[i][0] + " <- " + pairs[i][1] + pairs[i][3];
            assertRejectedWith(description,
                    () -> env.executeSql("INSERT INTO " + flinkTable + " VALUES (1, " + value + ")"),
                    needles(pairs[i][4]));
        }
    }

    /** Flink SQL has no MULTISET literal, so this one pair needs COLLECT rather than the table above. */
    @Test
    void multisetIntoANonUInt64MapValueIsRejectedAtPlanning() throws Exception {
        String table = "table_api_multiset_reject";
        createTable(table, "id Int64, tags Map(String, Int64)");

        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        env.executeSql(sinkDdl("ch_multiset_reject", table,
                "id BIGINT NOT NULL, tags MULTISET<STRING NOT NULL> NOT NULL"));

        assertFailsWith(() -> env.executeSql("INSERT INTO ch_multiset_reject "
                        + "SELECT CAST(id AS BIGINT), COLLECT(CAST(tag AS STRING)) "
                        + "FROM (VALUES (1, 'a')) AS t(id, tag) GROUP BY id"),
                "Column 'tags'", "MULTISET counts require a Map value type of exactly UInt64, found Int64");
    }

    /**
     * Every check the sink defers to write time, at one step past the bound: the value the types
     * alone cannot rule out fails per record, naming the column — and, inside a composite, the
     * path within it ({@code c element}, {@code c value}, {@code c.a}).
     */
    @Test
    void everyDeferredValueCheckFailsPerRecordNamingItsPath() throws Exception {
        String[][] cases = {
            // ClickHouse column type | Flink type of 'c' | out-of-range value | expected message fragments
            {"Date", "DATE NOT NULL", "DATE '1969-12-31'",
             "Column 'c': DATE value 1969-12-31 is outside the ClickHouse Date range"},
            {"DateTime", "TIMESTAMP(0) NOT NULL", "TIMESTAMP '2106-02-07 06:28:16'",
             "Column 'c'~is outside the ClickHouse DateTime range"},
            {"DateTime64(3)", "TIMESTAMP(3) NOT NULL", "TIMESTAMP '1899-12-31 23:59:59.999'",
             "Column 'c'~is outside the ClickHouse DateTime64 range"},
            {"UInt8", "BIGINT NOT NULL", "CAST(256 AS BIGINT)", "Column 'c': value 256 is outside the UInt8 range"},
            {"UInt64", "DECIMAL(20, 0) NOT NULL", "CAST('-1' AS DECIMAL(20, 0))",
             "Column 'c': value -1 is negative and cannot be written to the unsigned type UInt64"},
            {"Int32", "DECIMAL(10, 0) NOT NULL", "CAST(2147483648 AS DECIMAL(10, 0))",
             "Column 'c': value 2147483648 is outside the Int32 range"},
            {"FixedString(2)", "STRING NOT NULL", "'abc'", "Column 'c': value of 3 bytes does not fit FixedString(2)"},
            {"UUID", "STRING NOT NULL", "'not-a-uuid'", "Column 'c': value is not a valid UUID: not-a-uuid"},
            // the same checks, reached through each composite position
            {"Array(UInt64)", "ARRAY<BIGINT NOT NULL> NOT NULL", "ARRAY[CAST(-1 AS BIGINT)]",
             "Column 'c element': value -1 is outside the UInt64 range"},
            {"Array(FixedString(2))", "ARRAY<STRING NOT NULL> NOT NULL", "ARRAY['abc']",
             "Column 'c element': value of 3 bytes does not fit FixedString(2)"},
            {"Map(String, UInt32)", "MAP<STRING NOT NULL, BIGINT NOT NULL> NOT NULL", "MAP['k', CAST(-1 AS BIGINT)]",
             "Column 'c value': value -1 is outside the UInt32 range"},
            {"Tuple(a UInt8, b String)", "ROW<a BIGINT NOT NULL, b STRING NOT NULL> NOT NULL",
             "ROW(CAST(256 AS BIGINT), 'x')", "Column 'c.a': value 256 is outside the UInt8 range"},
            // nulls the header cannot encode inside a composite
            {"Map(String, Int32)", "MAP<STRING NOT NULL, INT NOT NULL> NOT NULL", "MAP['k', CAST(NULL AS INT)]",
             "Column 'c': null map value cannot be written to a non-Nullable ClickHouse Map value type"},
            {"Map(String, Int32)", "MAP<STRING NOT NULL, INT NOT NULL> NOT NULL", "MAP[CAST(NULL AS STRING), 1]",
             "Column 'c': null map key cannot be written to ClickHouse"},
        };

        TableEnvironment env = tableEnvironment();
        for (int i = 0; i < cases.length; i++) {
            String table = rejectionTable("value_check", i, cases[i][0]);
            String flinkTable = "ch_value_check_" + i;
            String value = cases[i][2];
            env.executeSql(sinkDdl(flinkTable, table, "id BIGINT NOT NULL, c " + cases[i][1]));

            String description = cases[i][0] + " <- " + cases[i][1] + " value " + value;
            assertRejectedWith(description,
                    () -> env.executeSql("INSERT INTO " + flinkTable + " VALUES (1, " + value + ")").await(),
                    needles(cases[i][3]));
            // The check runs in the sink, so the statement planned and nothing reached the table.
            Assertions.assertEquals(0, readBack("id", table, "id", 0).size(), description + " wrote a row");
        }
    }

    /** The payload map reserves one key name; a sink column colliding with it is refused up front. */
    @Test
    void reservedPayloadKeyColumnIsRejectedAtPlanning() throws Exception {
        String table = "table_api_reserved_key";
        createTable(table, "id Int64, `__clickhouse_raw__` String");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_reserved_key", table,
                "id BIGINT NOT NULL, `__clickhouse_raw__` STRING NOT NULL"));

        assertFailsWith(() -> env.executeSql("INSERT INTO ch_reserved_key VALUES (1, 'x')"),
                "collides with the connector's reserved payload key");
    }

    /** Dropping every unknown column leaves nothing to insert, which is a schema error, not an empty write. */
    @Test
    void ignoringEveryFlinkColumnLeavesNothingToInsert() throws Exception {
        String table = "table_api_nothing_to_insert";
        createTable(table, "id Int64");

        TableEnvironment env = tableEnvironment();
        env.executeSql(sinkDdl("ch_nothing", table, "nickname STRING NOT NULL",
                ", 'sink.ignore-unknown-flink-columns' = 'true'"));

        assertFailsWith(() -> env.executeSql("INSERT INTO ch_nothing VALUES ('nick')"),
                "None of the Flink schema columns map to ClickHouse table", "nothing to insert");
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
