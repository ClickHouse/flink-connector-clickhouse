package org.apache.flink.connector.clickhouse.table.schema;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The catalog's reverse direction of the matrix: ClickHouse column → declarable Flink type. */
class ClickHouseTypeMapperReverseTest {

    private static ClickHouseColumn col(String type) {
        return ClickHouseColumn.of("c", type);
    }

    /** One column per reverse-matrix row (scalars, wrappers, composites, nesting). */
    private static final String ROUND_TRIP_COLUMNS =
            "b Bool, i8 Int8, i16 Int16, i32 Int32, i64 Int64, i128 Int128, i256 Int256, "
            + "u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64, u128 UInt128, u256 UInt256, "
            + "f32 Float32, f64 Float64, dec Decimal(18, 4), dec32 Decimal32(2), dec256 Decimal256(6), "
            + "s String, fs FixedString(16), uid UUID, d Date, d32 Date32, "
            + "dt DateTime, dt_tz DateTime('UTC'), dt64 DateTime64(3), dt64_tz DateTime64(6, 'Asia/Tokyo'), "
            + "ns Nullable(String), ni Nullable(Int64), nu Nullable(UInt32), "
            + "lc LowCardinality(String), lcn LowCardinality(Nullable(String)), "
            + "saf SimpleAggregateFunction(sum, Int64), "
            + "arr Array(String), arr_n Array(Nullable(Int64)), arr_arr Array(Array(Float64)), "
            + "m Map(String, Int64), m_comp Map(UInt64, Array(String)), "
            + "t Tuple(String, Int32), t_named Tuple(lat Float64, lon Float64)";

    /**
     * The correctness invariant of the reverse matrix: every schema it emits passes
     * {@code SchemaResolver} against the very columns it came from.
     */
    @Test
    void everyEmittedSchemaResolvesAgainstItsOwnColumns() {
        List<ClickHouseColumn> columns = ClickHouseColumn.parse(ROUND_TRIP_COLUMNS);
        List<Column> flinkColumns = columns.stream()
                .map(column -> Column.physical(
                        column.getColumnName(), ClickHouseTypeMapper.toFlinkType(column)))
                .collect(Collectors.toList());

        List<ResolvedColumnMapping> mappings = SchemaResolver.resolve(
                ResolvedSchema.of(flinkColumns),
                new TableSchema(columns),
                new SchemaResolverOptions("analytics", "events", ZoneId.of("UTC"), false));

        assertEquals(columns.size(), mappings.size());
    }

    @Test
    void scalarsMapToTheTypeAUserWouldDeclare() {
        assertEquals(DataTypes.BIGINT().notNull(), ClickHouseTypeMapper.toFlinkType(col("Int64")));
        assertEquals(DataTypes.BIGINT().notNull(), ClickHouseTypeMapper.toFlinkType(col("UInt32")));
        assertEquals(DataTypes.DECIMAL(20, 0).notNull(), ClickHouseTypeMapper.toFlinkType(col("UInt64")));
        assertEquals(DataTypes.DECIMAL(38, 0).notNull(), ClickHouseTypeMapper.toFlinkType(col("Int128")));
        assertEquals(DataTypes.STRING().notNull(), ClickHouseTypeMapper.toFlinkType(col("FixedString(8)")));
        assertEquals(DataTypes.DATE().notNull(), ClickHouseTypeMapper.toFlinkType(col("Date32")));
        assertEquals(DataTypes.TIMESTAMP_LTZ(0).notNull(), ClickHouseTypeMapper.toFlinkType(col("DateTime")));
        assertEquals(DataTypes.TIMESTAMP_LTZ(3).notNull(),
                ClickHouseTypeMapper.toFlinkType(col("DateTime64(3, 'UTC')")));
    }

    @Test
    void wrappersAreUnwrappedAndNullabilityIsCarried() {
        assertEquals(DataTypes.BIGINT(), ClickHouseTypeMapper.toFlinkType(col("Nullable(Int64)")));
        assertEquals(DataTypes.STRING().notNull(),
                ClickHouseTypeMapper.toFlinkType(col("LowCardinality(String)")));
        assertEquals(DataTypes.STRING(),
                ClickHouseTypeMapper.toFlinkType(col("LowCardinality(Nullable(String))")));
        assertEquals(DataTypes.BIGINT().notNull(),
                ClickHouseTypeMapper.toFlinkType(col("SimpleAggregateFunction(sum, Int64)")));
    }

    /** DataWriter cannot write nulls to Nullable(UInt8/16/32/64) yet (issue #144). */
    @Test
    void nullableUnsignedTargetsAreClampedToNotNull() {
        assertEquals(DataTypes.BIGINT().notNull(),
                ClickHouseTypeMapper.toFlinkType(col("Nullable(UInt32)")));
    }

    @Test
    void widerDecimalPrecisionIsCappedNotRejected() {
        assertEquals(DataTypes.DECIMAL(38, 6).notNull(),
                ClickHouseTypeMapper.toFlinkType(col("Decimal256(6)")));
    }

    @Test
    void columnsOutsideTheMatrixAreRejectedWithTheForwardReason() {
        TypeMappingException e = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.toFlinkType(col("Enum8('new' = 1)")));
        assertEquals(TypeMappingException.Kind.TARGET_UNSUPPORTED, e.getKind());
        assertTrue(e.getMessage().contains("issue #43"), e.getMessage());
    }

    @Test
    void shapesTheSinkCannotWriteAreRejected() {
        assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.toFlinkType(col("Map(String, Nullable(String))")));
        assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.toFlinkType(col("Tuple(Nullable(Int64))")));
        assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.toFlinkType(col("Map(Float64, String)")));
    }
}
