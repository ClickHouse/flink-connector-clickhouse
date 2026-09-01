package org.apache.flink.connector.clickhouse.table.schema;

import com.clickhouse.data.ClickHouseColumn;

import org.apache.flink.connector.clickhouse.table.data.ValueConverter;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClickHouseTypeMapperTest {

    private static final ZoneId UTC = ZoneId.of("UTC");

    private static ClickHouseColumn col(String type) {
        return ClickHouseColumn.of("c", type);
    }

    /**
     * Guard: every Flink type root is either mapped or explicitly rejected, so a root
     * added by a future Flink can never fall through silently.
     */
    @Test
    void everyLogicalTypeRootIsMappedOrExplicitlyRejected() {
        assertEquals(EnumSet.allOf(LogicalTypeRoot.class),
                EnumSet.copyOf(ClickHouseTypeMapper.registeredRoots()));
    }

    @Test
    void intWritesToInt32AndWidensToInt64() {
        ValueConverter toInt32 = ClickHouseTypeMapper.converterFor(new IntType(false), col("Int32"), UTC, "c");
        assertEquals(7, toInt32.convert(7));
        ValueConverter toInt64 = ClickHouseTypeMapper.converterFor(new IntType(false), col("Int64"), UTC, "c");
        assertEquals(7L, toInt64.convert(7));
    }

    @Test
    void narrowingIntToInt16IsRejected() {
        assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(new IntType(false), col("Int16"), UTC, "c"));
    }

    @Test
    void signedIntNeverTargetsUnsignedOfSameWidth() {
        assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(new IntType(false), col("UInt32"), UTC, "c"));
    }

    @Test
    void stringConvertsToUuidForUuidColumns() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                new VarCharType(false, VarCharType.MAX_LENGTH), col("UUID"), UTC, "c");
        UUID uuid = UUID.randomUUID();
        assertEquals(uuid, converter.convert(StringData.fromString(uuid.toString())));
        assertEquals(uuid, converter.convert(
                StringData.fromString(uuid.toString().toUpperCase())));
    }

    @Test
    void overlongStringIntoFixedStringFailsNamingTheColumn() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                new VarCharType(false, VarCharType.MAX_LENGTH), col("FixedString(4)"), UTC, "c");
        assertEquals("abcd", converter.convert(StringData.fromString("abcd")));
        assertEquals("ab", converter.convert(StringData.fromString("ab")));
        // Three chars but six UTF-8 bytes — the limit is bytes, not characters.
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> converter.convert(StringData.fromString("ééé")));
        assertTrue(e.getMessage().contains("Column 'c'"), e.getMessage());
        assertTrue(e.getMessage().contains("FixedString(4)"), e.getMessage());
    }

    @Test
    void nonCanonicalUuidTextIsRejectedNamingTheColumn() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                new VarCharType(false, VarCharType.MAX_LENGTH), col("UUID"), UTC, "c");
        // UUID.fromString would silently zero-expand this to 00000001-0001-...-000000000001.
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> converter.convert(StringData.fromString("1-1-1-1-1")));
        assertTrue(e.getMessage().contains("Column 'c'"), e.getMessage());
        assertTrue(e.getMessage().contains("not a valid UUID"), e.getMessage());
    }

    @Test
    void enumTargetIsExplicitlyUnsupported() {
        TypeMappingException e = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(
                        new VarCharType(false, VarCharType.MAX_LENGTH),
                        col("Enum8('new' = 1, 'done' = 2)"), UTC, "c"));
        assertEquals(TypeMappingException.Kind.TARGET_UNSUPPORTED, e.getKind());
        assertTrue(e.getMessage().contains("issue #43"), e.getMessage());
    }

    @Test
    void timestampPrecisionMayNotExceedColumnScale() {
        TypeMappingException e = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(
                        new TimestampType(false, 9), col("DateTime64(3)"), UTC, "c"));
        assertEquals("precision 9 exceeds the column's scale 3", e.getMessage());
    }

    @Test
    void timestampIsInterpretedInTheSinkTimezone() {
        ZoneId tokyo = ZoneId.of("Asia/Tokyo");
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                new TimestampType(false, 3), col("DateTime64(3)"), tokyo, "c");
        LocalDateTime wallClock = LocalDateTime.of(2026, 1, 2, 3, 4, 5, 678_000_000);
        assertEquals(ZonedDateTime.of(wallClock, tokyo),
                converter.convert(TimestampData.fromLocalDateTime(wallClock)));
    }

    @Test
    void dstGapAndOverlapResolveAsDocumented() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                new TimestampType(false, 3), col("DateTime64(3)"),
                ZoneId.of("America/New_York"), "c");
        // 02:30 does not exist on 2026-03-08 (spring forward): shifted an hour ahead.
        ZonedDateTime gap = (ZonedDateTime) converter.convert(
                TimestampData.fromLocalDateTime(LocalDateTime.of(2026, 3, 8, 2, 30)));
        assertEquals(Instant.parse("2026-03-08T07:30:00Z"), gap.toInstant());
        // 01:30 occurs twice on 2026-11-01 (fall back): the earlier (-04:00) pass wins.
        ZonedDateTime overlap = (ZonedDateTime) converter.convert(
                TimestampData.fromLocalDateTime(LocalDateTime.of(2026, 11, 1, 1, 30)));
        assertEquals(Instant.parse("2026-11-01T05:30:00Z"), overlap.toInstant());
    }

    @Test
    void simpleAggregateFunctionIsTransparentForMatching() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                new IntType(false), col("SimpleAggregateFunction(max, Int32)"), UTC, "c");
        assertEquals(41, converter.convert(41));
    }

    @Test
    void multisetWritesElementCountsAsLongs() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                multisetOfString(), col("Map(String, UInt64)"), UTC, "c");
        Map<Object, Object> counts = new LinkedHashMap<>();
        counts.put(StringData.fromString("a"), 2);
        assertEquals(Map.of("a", 2L), converter.convert(new GenericMapData(counts)));
    }

    @Test
    void multisetRequiresUInt64CountColumns() {
        TypeMappingException e = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(
                        multisetOfString(), col("Map(String, UInt32)"), UTC, "c"));
        assertTrue(e.getMessage().contains("exactly UInt64"), e.getMessage());
    }

    @Test
    void unsignedTargetsRejectSignAndOverflowNamingTheColumn() {
        ValueConverter toUInt8 = ClickHouseTypeMapper.converterFor(
                new SmallIntType(false), col("UInt8"), UTC, "c");
        assertEquals(255, toUInt8.convert((short) 255));
        assertRangeError(() -> toUInt8.convert((short) -1), "UInt8 range 0..255");
        assertRangeError(() -> toUInt8.convert((short) 256), "UInt8 range 0..255");

        ValueConverter toUInt16 = ClickHouseTypeMapper.converterFor(
                new IntType(false), col("UInt16"), UTC, "c");
        assertEquals(65535, toUInt16.convert(65535));
        assertRangeError(() -> toUInt16.convert(-1), "UInt16 range 0..65535");
        assertRangeError(() -> toUInt16.convert(65536), "UInt16 range 0..65535");

        ValueConverter toUInt32 = ClickHouseTypeMapper.converterFor(
                new BigIntType(false), col("UInt32"), UTC, "c");
        assertEquals(4294967295L, toUInt32.convert(4294967295L));
        assertRangeError(() -> toUInt32.convert(-1L), "UInt32 range 0..4294967295");
        assertRangeError(() -> toUInt32.convert(4294967296L), "UInt32 range 0..4294967295");
    }

    @Test
    void decimalToUInt64IsRangeCheckedPerRecord() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                new DecimalType(false, 20, 0), col("UInt64"), UTC, "c");
        assertEquals(new BigInteger("18446744073709551615"),
                converter.convert(decimal("18446744073709551615")));
        // 20 digits pass the planning precision check but exceed UInt64's maximum.
        assertRangeError(() -> converter.convert(decimal("99999999999999999999")), "UInt64 range");
        assertRangeError(() -> converter.convert(decimal("-1")), "unsigned type UInt64");
    }

    @Test
    void nestedUnsignedValuesAreRangeCheckedToo() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                new ArrayType(false, new BigIntType(false)), col("Array(UInt32)"), UTC, "c");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> converter.convert(new GenericArrayData(new long[]{1L, -1L})));
        assertTrue(e.getMessage().contains("Column 'c element'"), e.getMessage());
    }

    @Test
    void rowWritesToTupleWithPositionalFields() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                rowOf(new IntType(false), new VarCharType(false, VarCharType.MAX_LENGTH)),
                col("Tuple(Int32, String)"), UTC, "c");
        Object[] tuple = (Object[]) converter.convert(
                GenericRowData.of(7, StringData.fromString("x")));
        assertArrayEquals(new Object[]{7, "x"}, tuple);
    }

    @Test
    void rowFieldCountMustMatchTupleElementCount() {
        TypeMappingException e = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(
                        rowOf(new IntType(false)), col("Tuple(Int32, String)"), UTC, "c"));
        assertEquals("ROW has 1 fields but the Tuple has 2 elements", e.getMessage());
    }

    @Test
    void nullableTupleElementsAreRejectedOnEitherSide() {
        TypeMappingException flinkSide = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(
                        rowOf(new IntType(true)), col("Tuple(Int32)"), UTC, "c"));
        assertTrue(flinkSide.getMessage().contains("declare the field NOT NULL"),
                flinkSide.getMessage());

        TypeMappingException clickHouseSide = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(
                        rowOf(new IntType(false)), col("Tuple(Nullable(Int32))"), UTC, "c"));
        assertTrue(clickHouseSide.getMessage().contains("Nullable Tuple elements"),
                clickHouseSide.getMessage());
    }

    @Test
    void nullRowFieldFailsNamingTheColumn() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                rowOf(new IntType(false), new VarCharType(false, VarCharType.MAX_LENGTH)),
                col("Tuple(Int32, String)"), UTC, "c");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> converter.convert(GenericRowData.of(7, null)));
        assertTrue(e.getMessage().contains("Column 'c'"), e.getMessage());
        assertTrue(e.getMessage().contains("null ROW field 2"), e.getMessage());
    }

    @Test
    void rowFieldRangeChecksNameTheFieldPath() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                rowOf(new SmallIntType(false)), col("Tuple(UInt8)"), UTC, "c");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> converter.convert(GenericRowData.of((short) 256)));
        assertTrue(e.getMessage().contains("Column 'c.f0'"), e.getMessage());
        assertTrue(e.getMessage().contains("UInt8 range 0..255"), e.getMessage());
    }

    @Test
    void multisetRejectsNegativeCounts() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                multisetOfString(), col("Map(String, UInt64)"), UTC, "c");
        Map<Object, Object> counts = new LinkedHashMap<>();
        counts.put(StringData.fromString("a"), -1);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> converter.convert(new GenericMapData(counts)));
        assertTrue(e.getMessage().contains("MULTISET count -1"), e.getMessage());
    }

    @Test
    void uInt64MapKeysAreRejectedAtPlanning() {
        // client-v2 hardcodes Long.parseLong for UInt64 map keys, so keys above 2^63-1 fail.
        MapType mapType = new MapType(false,
                new DecimalType(false, 20, 0), new VarCharType(false, VarCharType.MAX_LENGTH));
        TypeMappingException e = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(mapType, col("Map(UInt64, String)"), UTC, "c"));
        assertTrue(e.getMessage().contains("Map keys of type UInt64"), e.getMessage());
        assertTrue(e.getMessage().contains("upper half of the UInt64 range"), e.getMessage());

        TypeMappingException multisetError = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(
                        new MultisetType(false, new DecimalType(false, 20, 0)),
                        col("Map(UInt64, UInt64)"), UTC, "c"));
        assertTrue(multisetError.getMessage().contains("Map keys of type UInt64"),
                multisetError.getMessage());
    }

    @Test
    void uInt128MapKeysStayAccepted() {
        // UInt128 keys use client-v2's BigInteger parse — the full range round-trips.
        MapType mapType = new MapType(false,
                new DecimalType(false, 38, 0), new VarCharType(false, VarCharType.MAX_LENGTH));
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                mapType, col("Map(UInt128, String)"), UTC, "c");
        Map<Object, Object> entries = new LinkedHashMap<>();
        entries.put(DecimalData.fromBigDecimal(new BigDecimal("18446744073709551616"), 38, 0),
                StringData.fromString("v"));
        assertEquals(Map.of("18446744073709551616", "v"),
                converter.convert(new GenericMapData(entries)));
    }

    @Test
    void date32IsRangeCheckedPerRecord() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                new DateType(false), col("Date32"), UTC, "c");
        assertEquals(LocalDate.of(1900, 1, 1),
                converter.convert((int) LocalDate.of(1900, 1, 1).toEpochDay()));
        assertEquals(LocalDate.of(2299, 12, 31),
                converter.convert((int) LocalDate.of(2299, 12, 31).toEpochDay()));
        assertRangeError(() -> converter.convert((int) LocalDate.of(9999, 12, 31).toEpochDay()),
                "Date32 range 1900-01-01..2299-12-31");
        assertRangeError(() -> converter.convert((int) LocalDate.of(1899, 12, 31).toEpochDay()),
                "Date32 range");
    }

    @Test
    void dateTimeRejectsInstantsOutsideUInt32Seconds() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                new TimestampType(false, 0), col("DateTime"), UTC, "c");
        converter.convert(TimestampData.fromLocalDateTime(LocalDateTime.of(1970, 1, 1, 0, 0)));
        converter.convert(TimestampData.fromLocalDateTime(LocalDateTime.of(2106, 2, 7, 6, 28, 15)));
        assertRangeError(() -> converter.convert(
                        TimestampData.fromLocalDateTime(LocalDateTime.of(1969, 12, 31, 23, 0))),
                "DateTime range");
        assertRangeError(() -> converter.convert(
                        TimestampData.fromLocalDateTime(LocalDateTime.of(2106, 2, 7, 6, 28, 16))),
                "DateTime range");
    }

    @Test
    void dateTime64RejectsInstantsOutsideItsDocumentedRange() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                new TimestampType(false, 3), col("DateTime64(3)"), UTC, "c");
        converter.convert(TimestampData.fromLocalDateTime(LocalDateTime.of(1900, 1, 1, 0, 0)));
        converter.convert(TimestampData.fromLocalDateTime(LocalDateTime.of(2299, 12, 31, 23, 59, 59)));
        assertRangeError(() -> converter.convert(
                        TimestampData.fromLocalDateTime(LocalDateTime.of(1899, 12, 31, 23, 59))),
                "DateTime64 range");
        assertRangeError(() -> converter.convert(
                        TimestampData.fromLocalDateTime(LocalDateTime.of(9999, 12, 31, 0, 0))),
                "DateTime64 range");
    }

    @Test
    void dateTime64Scale9CapsAtInt64TickRange() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                new TimestampType(false, 9), col("DateTime64(9)"), UTC, "c");
        converter.convert(TimestampData.fromLocalDateTime(LocalDateTime.of(2262, 4, 11, 0, 0)));
        // Inside the documented 2299 bound, but its scale-9 ticks overflow Int64.
        assertRangeError(() -> converter.convert(
                        TimestampData.fromLocalDateTime(LocalDateTime.of(2263, 1, 1, 0, 0))),
                "DateTime64 range");
    }

    private static void assertRangeError(Executable call, String expectedFragment) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, call);
        assertTrue(e.getMessage().contains("Column 'c'"), e.getMessage());
        assertTrue(e.getMessage().contains(expectedFragment), e.getMessage());
    }

    private static DecimalData decimal(String unscaled) {
        return DecimalData.fromBigDecimal(new BigDecimal(unscaled), 20, 0);
    }

    private static MultisetType multisetOfString() {
        return new MultisetType(false, new VarCharType(false, VarCharType.MAX_LENGTH));
    }

    private static RowType rowOf(LogicalType... fieldTypes) {
        List<RowType.RowField> fields = new ArrayList<>();
        for (int i = 0; i < fieldTypes.length; i++) {
            fields.add(new RowType.RowField("f" + i, fieldTypes[i]));
        }
        return new RowType(false, fields);
    }
}
