package org.apache.flink.connector.clickhouse.table.schema;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;

import org.apache.flink.connector.clickhouse.table.data.RowDataDataMapper;
import org.apache.flink.connector.clickhouse.convertor.ColumnBinding;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaResolverTest {

    private static final ZoneId UTC = ZoneId.of("UTC");

    private static SchemaResolverOptions options(boolean ignoreUnknownColumns) {
        return new SchemaResolverOptions("analytics", "events", UTC, ignoreUnknownColumns);
    }

    private static TableSchema clickHouseSchema(String columnList) {
        return new TableSchema(ClickHouseColumn.parse(columnList));
    }

    /** Schema exercising scalars, UUID pairing, composites and reordering. */
    private static ResolvedSchema flinkSchema() {
        return ResolvedSchema.of(
                Column.physical("id", DataTypes.BIGINT().notNull()),
                Column.physical("name", DataTypes.STRING().notNull()),
                Column.physical("amount", DataTypes.DECIMAL(18, 4).notNull()),
                Column.physical("created_at", DataTypes.TIMESTAMP(3).notNull()),
                Column.physical("uid", DataTypes.STRING().notNull()),
                Column.physical("tags", DataTypes.ARRAY(DataTypes.STRING().notNull()).notNull()),
                Column.physical("props",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING().notNull()).notNull()));
    }

    /** Deliberately not in Flink order — the mapping order must come from the Flink schema. */
    private static final String CH_COLUMNS =
            "uid UUID, id Int64, name String, amount Decimal(18, 4), "
            + "created_at DateTime64(3), tags Array(String), props Map(String, String)";

    @Test
    void resolvesColumnsInFlinkOrderWithCanonicalTypeExpressions() {
        List<ResolvedColumnMapping> mappings = SchemaResolver.resolve(
                flinkSchema(), clickHouseSchema(CH_COLUMNS), options(false));

        assertEquals(List.of("id", "name", "amount", "created_at", "uid", "tags", "props"),
                mappings.stream().map(ResolvedColumnMapping::columnName).collect(Collectors.toList()));
        assertEquals(List.of("Int64", "String", "Decimal(18, 4)", "DateTime64(3)", "UUID",
                        "Array(String)", "Map(String, String)"),
                mappings.stream().map(ResolvedColumnMapping::typeExpression).collect(Collectors.toList()));
    }

    @Test
    void mapperSurvivesSerializationAndProducesPlainJavaValues() throws Exception {
        RowDataDataMapper mapper = serializationRoundTrip(RowDataDataMapper.of(
                SchemaResolver.resolve(flinkSchema(), clickHouseSchema(CH_COLUMNS), options(false))));

        List<ColumnBinding> bindings = mapper.bindings();
        assertEquals(7, bindings.size());
        assertEquals("Int64", bindings.get(0).column.getOriginalTypeName());
        assertEquals("UUID", bindings.get(4).column.getOriginalTypeName());

        UUID uuid = UUID.randomUUID();
        LocalDateTime wallClock = LocalDateTime.of(2026, 1, 2, 3, 4, 5, 678_000_000);
        Map<Object, Object> props = new LinkedHashMap<>();
        props.put(StringData.fromString("k1"), StringData.fromString("v1"));
        GenericRowData row = GenericRowData.of(
                1L,
                StringData.fromString("alice"),
                DecimalData.fromBigDecimal(new BigDecimal("12.5000"), 18, 4),
                TimestampData.fromLocalDateTime(wallClock),
                StringData.fromString(uuid.toString()),
                new GenericArrayData(new Object[]{
                        StringData.fromString("a"), StringData.fromString("b")}),
                new GenericMapData(props));

        Map<String, Object> map = new HashMap<>();
        mapper.toMap(row, map);

        assertEquals(1L, map.get("id"));
        assertEquals("alice", map.get("name"));
        assertEquals(new BigDecimal("12.5000"), map.get("amount"));
        assertEquals(ZonedDateTime.of(wallClock, UTC), map.get("created_at"));
        assertEquals(uuid, map.get("uid"));
        assertEquals(List.of("a", "b"), map.get("tags"));
        assertEquals(Map.of("k1", "v1"), map.get("props"));
    }

    @Test
    void unknownFlinkColumnFailsWithPreciseMessage() {
        ResolvedSchema schema = ResolvedSchema.of(
                Column.physical("id", DataTypes.BIGINT().notNull()),
                Column.physical("nickname", DataTypes.STRING().notNull()));
        ValidationException e = assertThrows(ValidationException.class, () ->
                SchemaResolver.resolve(schema, clickHouseSchema("id Int64"), options(false)));
        assertTrue(e.getMessage().contains(
                "Column 'nickname' declared in the Flink schema does not exist in analytics.events"),
                e.getMessage());
        assertTrue(e.getMessage().contains("sink.ignore-unknown-flink-columns"), e.getMessage());
    }

    @Test
    void tableNamesNeedingQuotesAreRejectedBeforeIntrospection() {
        ValidationException e = assertThrows(ValidationException.class, () ->
                new SchemaResolverOptions("analytics", "my-table", UTC, false));
        assertTrue(e.getMessage().contains("'my-table'"), e.getMessage());
        assertTrue(e.getMessage().contains("unquoted"), e.getMessage());
        // Unusual but unquoted-legal names stay accepted.
        assertEquals("_events_2", new SchemaResolverOptions("analytics", "_events_2", UTC, false).table);
    }

    @Test
    void unknownFlinkColumnIsDroppedWhenIgnoreIsEnabled() {
        ResolvedSchema schema = ResolvedSchema.of(
                Column.physical("nickname", DataTypes.STRING().notNull()),
                Column.physical("id", DataTypes.BIGINT().notNull()));
        List<ResolvedColumnMapping> mappings = SchemaResolver.resolve(
                schema, clickHouseSchema("id Int64"), options(true));
        assertEquals(1, mappings.size());
        assertEquals("id", mappings.get(0).columnName());
        // The dropped column still occupies its position in the incoming row.
        GenericRowData row = GenericRowData.of(StringData.fromString("ignored"), 9L);
        assertEquals(9L, mappings.get(0).accessor.get(row));
    }

    @Test
    void reservedColumnNameIsDroppedLikeAnyUnknownColumnWhenIgnoreIsEnabled() {
        ResolvedSchema schema = ResolvedSchema.of(
                Column.physical("__clickhouse_raw__", DataTypes.STRING().notNull()),
                Column.physical("id", DataTypes.BIGINT().notNull()));
        List<ResolvedColumnMapping> mappings = SchemaResolver.resolve(
                schema, clickHouseSchema("id Int64"), options(true));
        assertEquals(List.of("id"),
                mappings.stream().map(ResolvedColumnMapping::columnName).collect(Collectors.toList()));
    }

    @Test
    void reservedColumnNameThatWouldMapIsRejected() {
        ResolvedSchema schema = ResolvedSchema.of(
                Column.physical("__clickhouse_raw__", DataTypes.STRING().notNull()));
        ValidationException e = assertThrows(ValidationException.class, () ->
                SchemaResolver.resolve(schema,
                        clickHouseSchema("__clickhouse_raw__ String"), options(true)));
        assertTrue(e.getMessage().contains("reserved"), e.getMessage());
    }

    @Test
    void nullableFlinkColumnCannotTargetNonNullableClickHouseColumn() {
        ResolvedSchema schema = ResolvedSchema.of(Column.physical("id", DataTypes.BIGINT()));
        ValidationException e = assertThrows(ValidationException.class, () ->
                SchemaResolver.resolve(schema, clickHouseSchema("id Int64"), options(false)));
        assertTrue(e.getMessage().contains("is not Nullable"), e.getMessage());
        // The relaxed direction works: nullable Flink column into a Nullable column.
        List<ResolvedColumnMapping> mappings = SchemaResolver.resolve(
                schema, clickHouseSchema("id Nullable(Int64)"), options(false));
        assertEquals("Nullable(Int64)", mappings.get(0).typeExpression());
    }

    /** The nullability hint is a dead end when the target type itself is unsupported, so type support is checked first. */
    @Test
    void unsupportedTargetIsReportedBeforeNullability() {
        ResolvedSchema schema = ResolvedSchema.of(Column.physical("status", DataTypes.STRING()));
        ValidationException e = assertThrows(ValidationException.class, () ->
                SchemaResolver.resolve(schema, clickHouseSchema("status Enum8('a' = 1)"), options(false)));
        assertTrue(e.getMessage().contains("not yet supported"), e.getMessage());
        assertFalse(e.getMessage().contains("is not Nullable"), e.getMessage());
    }

    /** Nullable(Array(...)) is invalid in ClickHouse, so the hint must not suggest it for composite columns. */
    @Test
    void nullableColumnHintIsDroppedForCompositeClickHouseTypes() {
        ResolvedSchema scalar = ResolvedSchema.of(Column.physical("id", DataTypes.BIGINT()));
        ValidationException scalarError = assertThrows(ValidationException.class, () ->
                SchemaResolver.resolve(scalar, clickHouseSchema("id Int64"), options(false)));
        assertTrue(scalarError.getMessage().endsWith("NOT NULL or make the ClickHouse column Nullable."),
                scalarError.getMessage());

        ResolvedSchema composite = ResolvedSchema.of(
                Column.physical("tags", DataTypes.ARRAY(DataTypes.STRING().notNull())));
        ValidationException compositeError = assertThrows(ValidationException.class, () ->
                SchemaResolver.resolve(composite, clickHouseSchema("tags Array(String)"), options(false)));
        assertTrue(compositeError.getMessage().endsWith("Declare the Flink column NOT NULL."),
                compositeError.getMessage());
    }

    @Test
    void narrowingFailsWithColumnAndBothTypesNamed() {
        ResolvedSchema schema = ResolvedSchema.of(Column.physical("v", DataTypes.INT().notNull()));
        ValidationException e = assertThrows(ValidationException.class, () ->
                SchemaResolver.resolve(schema, clickHouseSchema("v Int16"), options(false)));
        assertTrue(e.getMessage().startsWith(
                "Column 'v': Flink type INT NOT NULL cannot be written to ClickHouse column 'v Int16'"),
                e.getMessage());
    }

    @Test
    void nestedSimpleAggregateFunctionIsRejectedAtPlanning() {
        // client-v2 has no serializer case for SAF inside a composite; accepted schemas would
        // fail on the first record, so planning rejects them.
        ValidationException array = assertThrows(ValidationException.class, () ->
                SchemaResolver.resolve(
                        ResolvedSchema.of(Column.physical("v",
                                DataTypes.ARRAY(DataTypes.BIGINT().notNull()).notNull())),
                        clickHouseSchema("v Array(SimpleAggregateFunction(max, Int64))"),
                        options(false)));
        assertTrue(array.getMessage().contains("only writable as a top-level column"),
                array.getMessage());
        assertTrue(array.getMessage().contains("array element"), array.getMessage());

        ValidationException map = assertThrows(ValidationException.class, () ->
                SchemaResolver.resolve(
                        ResolvedSchema.of(Column.physical("m",
                                DataTypes.MAP(DataTypes.STRING().notNull(),
                                        DataTypes.BIGINT().notNull()).notNull())),
                        clickHouseSchema("m Map(String, SimpleAggregateFunction(max, Int64))"),
                        options(false)));
        assertTrue(map.getMessage().contains("map value"), map.getMessage());
    }

    @Test
    void omittedClickHouseColumnsAreAllowedRegardlessOfDefaults() {
        ResolvedSchema schema = ResolvedSchema.of(Column.physical("id", DataTypes.BIGINT().notNull()));
        // Nullable extra column: allowed, server default applies.
        SchemaResolver.resolve(schema, clickHouseSchema("id Int64, note Nullable(String)"), options(false));
        // Non-Nullable extra column without a DEFAULT: allowed too (warned) — the writer sets
        // input_format_defaults_for_omitted_fields=1, so the server fills the type default.
        SchemaResolver.resolve(schema, clickHouseSchema("id Int64, note String"), options(false));
        // And with a DEFAULT: allowed.
        TableSchema withDefault = clickHouseSchema("id Int64, note String");
        ClickHouseColumn note = withDefault.getColumnByName("note");
        note.setHasDefault(true);
        note.setDefaultValue(ClickHouseColumn.DefaultValue.DEFAULT);
        SchemaResolver.resolve(schema, withDefault, options(false));
    }

    @Test
    void materializedColumnsAreNotInsertable() {
        TableSchema schema = clickHouseSchema("id Int64, mat String");
        ClickHouseColumn mat = schema.getColumnByName("mat");
        mat.setHasDefault(true);
        mat.setDefaultValue(ClickHouseColumn.DefaultValue.MATERIALIZED);
        // Unmapped: fine, and excluded from the required-column check.
        SchemaResolver.resolve(ResolvedSchema.of(
                Column.physical("id", DataTypes.BIGINT().notNull())), schema, options(false));
        // Mapped: rejected.
        ValidationException e = assertThrows(ValidationException.class, () ->
                SchemaResolver.resolve(ResolvedSchema.of(
                        Column.physical("id", DataTypes.BIGINT().notNull()),
                        Column.physical("mat", DataTypes.STRING().notNull())), schema, options(false)));
        assertTrue(e.getMessage().contains("MATERIALIZED"), e.getMessage());
        assertTrue(e.getMessage().contains("the server computes it"), e.getMessage());
    }

    /** EPHEMERAL is insertable SQL-wise, but only via a column list the sink never sends. */
    @Test
    void ephemeralColumnsAreRejectedBecauseTheSinkSendsNoColumnList() {
        TableSchema schema = clickHouseSchema("id Int64, raw String");
        ClickHouseColumn raw = schema.getColumnByName("raw");
        raw.setHasDefault(true);
        raw.setDefaultValue(ClickHouseColumn.DefaultValue.EPHEMERAL);
        ValidationException e = assertThrows(ValidationException.class, () ->
                SchemaResolver.resolve(ResolvedSchema.of(
                        Column.physical("id", DataTypes.BIGINT().notNull()),
                        Column.physical("raw", DataTypes.STRING().notNull())), schema, options(false)));
        assertTrue(e.getMessage().contains("is EPHEMERAL"), e.getMessage());
        assertTrue(e.getMessage().contains("without a column list"), e.getMessage());
    }

    @Test
    void jsonColumnsAreDetectedForTheAutoEnable() {
        ResolvedSchema schema = ResolvedSchema.of(
                Column.physical("id", DataTypes.BIGINT().notNull()),
                Column.physical("payload", DataTypes.STRING().notNull()));
        List<ResolvedColumnMapping> withJson = SchemaResolver.resolve(
                schema, clickHouseSchema("id Int64, payload JSON"), options(false));
        assertTrue(SchemaResolver.targetsJsonColumn(withJson));
        List<ResolvedColumnMapping> withoutJson = SchemaResolver.resolve(
                schema, clickHouseSchema("id Int64, payload String"), options(false));
        assertFalse(SchemaResolver.targetsJsonColumn(withoutJson));
    }

    @SuppressWarnings("unchecked")
    private static <T> T serializationRoundTrip(T value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (T) in.readObject();
        }
    }
}
