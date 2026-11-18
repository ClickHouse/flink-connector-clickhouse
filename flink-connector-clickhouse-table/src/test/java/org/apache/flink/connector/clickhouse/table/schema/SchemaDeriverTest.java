package org.apache.flink.connector.clickhouse.table.schema;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Pins the ClickHouse to Flink mapping of the catalog, and that the sink accepts every schema it derives. */
class SchemaDeriverTest {

    private static final String TABLE = "analytics.events";

    private static TableSchema clickHouseSchema(String... types) {
        List<ClickHouseColumn> columns = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            columns.add(ClickHouseColumn.of("c" + i, types[i]));
        }
        return new TableSchema(columns);
    }

    private static LogicalType derived(String clickHouseType) {
        return SchemaDeriver.derive(clickHouseSchema(clickHouseType), TABLE)
                .getColumnDataTypes().get(0).getLogicalType();
    }

    private static LogicalType flink(String sql) {
        return LogicalTypeParser.parse(sql, SchemaDeriverTest.class.getClassLoader());
    }

    private static final String[][] COUNTERPARTS = {
            // ClickHouse column                                 Flink type
            {"Bool",                                              "BOOLEAN NOT NULL"},
            {"Int8",                                              "TINYINT NOT NULL"},
            {"Int16",                                             "SMALLINT NOT NULL"},
            {"Int32",                                             "INT NOT NULL"},
            {"Int64",                                             "BIGINT NOT NULL"},
            {"Int128",                                            "DECIMAL(38, 0) NOT NULL"},
            {"Int256",                                            "DECIMAL(38, 0) NOT NULL"},
            // unsigned: the narrowest signed Flink type holding the whole range
            {"UInt8",                                             "SMALLINT NOT NULL"},
            {"UInt16",                                            "INT NOT NULL"},
            {"UInt32",                                            "BIGINT NOT NULL"},
            {"UInt64",                                            "DECIMAL(20, 0) NOT NULL"},
            {"UInt128",                                           "DECIMAL(38, 0) NOT NULL"},
            {"UInt256",                                           "DECIMAL(38, 0) NOT NULL"},
            {"Float32",                                           "FLOAT NOT NULL"},
            {"Float64",                                           "DOUBLE NOT NULL"},
            {"Decimal(18, 4)",                                    "DECIMAL(18, 4) NOT NULL"},
            {"Decimal32(2)",                                      "DECIMAL(9, 2) NOT NULL"},
            {"Decimal64(4)",                                      "DECIMAL(18, 4) NOT NULL"},
            {"Decimal128(10)",                                    "DECIMAL(38, 10) NOT NULL"},
            // beyond Flink's 38 digits the integer digits come first, and the scale keeps at least min(s, 6)
            {"Decimal(76, 50)",                                   "DECIMAL(38, 12) NOT NULL"},
            {"Decimal(76, 38)",                                   "DECIMAL(38, 6) NOT NULL"},
            {"Decimal256(20)",                                    "DECIMAL(38, 6) NOT NULL"},
            {"Decimal(50, 2)",                                    "DECIMAL(38, 2) NOT NULL"},
            {"String",                                            "STRING NOT NULL"},
            {"FixedString(8)",                                    "STRING NOT NULL"},
            {"UUID",                                              "STRING NOT NULL"},
            {"JSON",                                              "STRING NOT NULL"},
            {"Date",                                              "DATE NOT NULL"},
            {"Date32",                                            "DATE NOT NULL"},
            // a column naming its zone is an instant, one without is a wall clock
            {"DateTime",                                          "TIMESTAMP(0) NOT NULL"},
            {"DateTime('UTC')",                                   "TIMESTAMP_LTZ(0) NOT NULL"},
            {"DateTime64(3)",                                     "TIMESTAMP(3) NOT NULL"},
            {"DateTime64(9)",                                     "TIMESTAMP(9) NOT NULL"},
            {"DateTime64(6, 'Europe/Berlin')",                    "TIMESTAMP_LTZ(6) NOT NULL"},
            // wrappers: Nullable decides nullability, the others are transparent
            {"Nullable(Int32)",                                   "INT"},
            {"Nullable(DateTime64(3, 'UTC'))",                    "TIMESTAMP_LTZ(3)"},
            {"LowCardinality(String)",                            "STRING NOT NULL"},
            {"LowCardinality(Nullable(String))",                  "STRING"},
            {"SimpleAggregateFunction(anyLast, String)",          "STRING NOT NULL"},
            {"SimpleAggregateFunction(sum, Nullable(Int64))",     "BIGINT"},
            // issue #144: the sink cannot write a null to these yet
            {"Nullable(UInt8)",                                   "SMALLINT NOT NULL"},
            {"Nullable(UInt16)",                                  "INT NOT NULL"},
            {"Nullable(UInt32)",                                  "BIGINT NOT NULL"},
            {"Nullable(UInt64)",                                  "DECIMAL(20, 0) NOT NULL"},
            {"Nullable(UInt128)",                                 "DECIMAL(38, 0)"},
            // composites: only an Array element can be Nullable, and a MAP key stays nullable as in Flink DDL
            {"Array(String)",                                     "ARRAY<STRING NOT NULL> NOT NULL"},
            {"Array(Nullable(UInt8))",                            "ARRAY<SMALLINT> NOT NULL"},
            {"Array(Array(Int64))",                               "ARRAY<ARRAY<BIGINT NOT NULL> NOT NULL> NOT NULL"},
            {"Map(String, Int64)",                                "MAP<STRING, BIGINT NOT NULL> NOT NULL"},
            {"Map(Int32, Array(String))",                         "MAP<INT, ARRAY<STRING NOT NULL> NOT NULL> NOT NULL"},
            {"Map(LowCardinality(String), UInt64)",               "MAP<STRING, DECIMAL(20, 0) NOT NULL> NOT NULL"},
            {"Tuple(Int32, String)",                              "ROW<f0 INT NOT NULL, f1 STRING NOT NULL> NOT NULL"},
            {"Tuple(a Int32, b String)",                          "ROW<a INT NOT NULL, b STRING NOT NULL> NOT NULL"},
            {"Array(Tuple(id UInt32, at DateTime64(3, 'UTC')))",
                    "ARRAY<ROW<id BIGINT NOT NULL, `at` TIMESTAMP_LTZ(3) NOT NULL> NOT NULL> NOT NULL"},
    };

    private static Stream<Arguments> counterparts() {
        return Stream.of(COUNTERPARTS).map(row -> Arguments.of(row[0], row[1]));
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("counterparts")
    void everyWritableColumnDerivesItsFlinkCounterpart(String clickHouseType, String flinkType) {
        assertEquals(flink(flinkType), derived(clickHouseType));
    }

    /** The scalars the round trip below is built from: one spelling of every writable scalar type. */
    private static final List<String> SCALARS = List.of(
            "Bool", "Int8", "Int16", "Int32", "Int64", "Int128", "Int256",
            "UInt8", "UInt16", "UInt32", "UInt64", "UInt128", "UInt256", "Float32", "Float64",
            "Decimal(10, 2)", "Decimal32(2)", "Decimal64(4)", "Decimal128(10)", "Decimal256(20)",
            "String", "FixedString(8)", "UUID", "JSON", "Date", "Date32",
            "DateTime", "DateTime('UTC')", "DateTime64(3)", "DateTime64(9, 'Asia/Tokyo')");

    /** Map key types the sink restores from their string form. */
    private static final List<String> MAP_KEYS = List.of(
            "String", "FixedString(8)", "Int8", "Int16", "Int32", "Int64", "Int128", "Int256",
            "UInt8", "UInt16", "UInt32", "UInt128", "UInt256", "Decimal(10, 2)", "LowCardinality(String)");

    /** Every scalar bare and in each wrapper and composite position a ClickHouse table can hold it in. */
    private static Stream<String> writableColumns() {
        Stream<String> perScalar = SCALARS.stream().flatMap(t -> Stream.of(
                t,
                "Nullable(" + t + ")",
                "SimpleAggregateFunction(anyLast, " + t + ")",
                "SimpleAggregateFunction(anyLast, Nullable(" + t + "))",
                "Array(" + t + ")",
                "Array(Nullable(" + t + "))",
                "Array(Array(" + t + "))",
                "Map(String, " + t + ")",
                "Map(String, Array(Nullable(" + t + ")))",
                "Tuple(" + t + ", " + t + ")",
                "Tuple(a " + t + ", b Array(" + t + "))",
                "Array(Tuple(a " + t + ", b Map(String, " + t + ")))"));
        Stream<String> lowCardinality = Stream.of("String", "FixedString(8)", "Int32", "Date").flatMap(t -> Stream.of(
                "LowCardinality(" + t + ")",
                "LowCardinality(Nullable(" + t + "))",
                "Array(LowCardinality(" + t + "))"));
        Stream<String> keyed = MAP_KEYS.stream().map(k -> "Map(" + k + ", String)");
        return Stream.of(perScalar, lowCardinality, keyed).flatMap(s -> s);
    }

    /** A new writable type must join {@link #SCALARS}, or the round trip would not cover it. */
    @Test
    void theRoundTripCoversEveryWritableScalar() {
        Set<ClickHouseDataType> covered = SCALARS.stream()
                .map(type -> ClickHouseColumn.of("c", type).getDataType())
                .collect(Collectors.toSet());
        Set<ClickHouseDataType> writableScalars = EnumSet.copyOf(ClickHouseTypeMapper.WRITABLE_TARGETS);
        writableScalars.removeAll(EnumSet.of(
                ClickHouseDataType.Array, ClickHouseDataType.Map, ClickHouseDataType.Tuple));
        assertEquals(writableScalars, covered);
    }

    /** The catalog's contract: a derived schema resolves against the very columns it came from. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("writableColumns")
    void everyDerivedSchemaPassesTheSinksOwnResolution(String clickHouseType) {
        TableSchema clickHouseSchema = clickHouseSchema(clickHouseType);
        ResolvedSchema flinkSchema = SchemaDeriver.derive(clickHouseSchema, TABLE);

        List<ResolvedColumnMapping> mappings = SchemaResolver.resolve(flinkSchema, clickHouseSchema,
                new SchemaResolverOptions("analytics", "events", ZoneOffset.UTC, false));

        assertEquals(List.of("c0"), mappings.stream().map(ResolvedColumnMapping::columnName).collect(Collectors.toList()));
        assertEquals(clickHouseType, mappings.get(0).typeExpression());
    }

    private static final String[][] UNWRITABLE = {
            // ClickHouse column                         the reason it carries
            {"Enum8('a' = 1, 'b' = 2)",                   "see issue #43"},
            {"Variant(String, Int32)",                    "see issue #60"},
            {"Time",                                      "see issue #91"},
            {"IPv4",                                      "no write path"},
            {"Dynamic",                                   "no write path"},
            {"Point",                                     "no write path"},
            {"AggregateFunction(sum, Int64)",             "no write path"},
            {"Nested(a Int32, b String)",                 "Array(Tuple(...))"},
            {"Array(Enum8('a' = 1))",                     "see issue #43"},
            {"Map(UInt64, String)",                       "Map keys of type UInt64"},
            {"Map(UUID, String)",                         "cannot be restored from a string"},
            {"Map(String, Nullable(String))",             "clickhouse-java#2886"},
            {"Tuple(a Int32, b Nullable(String))",        "clickhouse-java#2886"},
            {"Array(SimpleAggregateFunction(sum, Int64))", "clickhouse-java#2937"},
    };

    private static Stream<Arguments> unwritable() {
        return Stream.of(UNWRITABLE).map(row -> Arguments.of(row[0], row[1]));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unwritable")
    void aColumnTheSinkCannotWriteFailsTheTableNamingIt(String clickHouseType, String reason) {
        ValidationException e = assertThrows(ValidationException.class,
                () -> SchemaDeriver.derive(clickHouseSchema("Int64", clickHouseType), TABLE));
        assertTrue(e.getMessage().contains("'c1 " + clickHouseType + "'"), e.getMessage());
        assertTrue(e.getMessage().contains(TABLE), e.getMessage());
        assertTrue(e.getMessage().contains(reason), e.getMessage());
        assertTrue(e.getMessage().contains("CREATE TEMPORARY TABLE"), e.getMessage());
    }

    /** No insert can carry these, so a schema naming them could never be written in full. */
    @Test
    void serverComputedColumnsAreLeftOut() {
        TableSchema schema = new TableSchema(ClickHouseColumn.parse(
                "id Int64, with_default String, materialized String, alias String, ephemeral String"));
        setDefault(schema, "with_default", ClickHouseColumn.DefaultValue.DEFAULT);
        setDefault(schema, "materialized", ClickHouseColumn.DefaultValue.MATERIALIZED);
        setDefault(schema, "alias", ClickHouseColumn.DefaultValue.ALIAS);
        setDefault(schema, "ephemeral", ClickHouseColumn.DefaultValue.EPHEMERAL);

        assertEquals(List.of("id", "with_default"), SchemaDeriver.derive(schema, TABLE).getColumnNames());
    }

    private static void setDefault(TableSchema schema, String column, ClickHouseColumn.DefaultValue kind) {
        schema.getColumnByName(column).setHasDefault(true);
        schema.getColumnByName(column).setDefaultValue(kind);
    }

    /** An unsupported type in a column no insert can carry must not fail the table. */
    @Test
    void anUnwritableTypeInAServerComputedColumnIsIgnored() {
        TableSchema schema = clickHouseSchema("Int64", "Enum8('a' = 1)");
        setDefault(schema, "c1", ClickHouseColumn.DefaultValue.MATERIALIZED);

        assertEquals(List.of("c0"), SchemaDeriver.derive(schema, TABLE).getColumnNames());
    }

    /** With the default {@code flatten_nested = 1} a Nested column arrives as dotted Array columns. */
    @Test
    void flattenedNestedColumnsKeepTheirDottedNames() {
        TableSchema schema = new TableSchema(List.of(
                ClickHouseColumn.of("id", "Int64"),
                ClickHouseColumn.of("n.a", "Array(Int32)"),
                ClickHouseColumn.of("n.b", "Array(String)")));

        ResolvedSchema derived = SchemaDeriver.derive(schema, TABLE);

        assertEquals(List.of("id", "n.a", "n.b"), derived.getColumnNames());
        assertEquals(flink("ARRAY<INT NOT NULL> NOT NULL"), derived.getColumnDataTypes().get(1).getLogicalType());
    }

    @Test
    void aColumnNamedLikeTheReservedPayloadKeyFailsTheTable() {
        TableSchema schema = new TableSchema(List.of(
                ClickHouseColumn.of("id", "Int64"),
                ClickHouseColumn.of("__clickhouse_raw__", "String")));

        ValidationException e = assertThrows(ValidationException.class, () -> SchemaDeriver.derive(schema, TABLE));
        assertTrue(e.getMessage().contains("reserved payload key"), e.getMessage());
    }
}
