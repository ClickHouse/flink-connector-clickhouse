package org.apache.flink.connector.clickhouse.table.schema;

import com.clickhouse.data.ClickHouseColumn;

import org.apache.flink.connector.clickhouse.table.data.ValueConverter;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the type matrix of the Table API sink. The matrix itself is {@link #MATRIX}, tested cell by
 * cell against every ClickHouse scalar column; composite types recurse into it and have their
 * structural and nullability rules tested separately below.
 */
class ClickHouseTypeMapperTest {

    private static final ZoneId UTC = ZoneId.of("UTC");
    private static final TypeMappingOptions LENIENT = new TypeMappingOptions(UTC, false);
    private static final TypeMappingOptions STRICT = new TypeMappingOptions(UTC, true);

    private static TypeMappingOptions lenientIn(ZoneId sinkTimezone) {
        return new TypeMappingOptions(sinkTimezone, false);
    }

    private static ClickHouseColumn col(String type) {
        return ClickHouseColumn.of("c", type);
    }

    /**
     * The type matrix, one row per Flink type. <b>planning check</b>: the types alone prove every
     * value fits, allowed with and without {@code sink.strict-type-mapping}. <b>runtime check</b>:
     * each value is checked at write time, so {@code sink.strict-type-mapping} rejects the pair at
     * planning. Every column not named in a row is rejected either way.
     */
    private static final String[] MATRIX = {
        //Flink type       | planning check: types alone prove fit   | runtime check: each value; rejected when strict
        "BOOLEAN          | Bool                                    | ",
        "TINYINT          | Int8 Int16 Int32 Int64 Int128 Int256    | UInt8 UInt16 UInt32 UInt64 UInt128 UInt256",
        "SMALLINT         | Int16 Int32 Int64 Int128 Int256         | Int8 UInt8 UInt16 UInt32 UInt64 UInt128 UInt256",
        "INT              | Int32 Int64 Int128 Int256               | Int8 Int16 UInt8 UInt16 UInt32 UInt64 UInt128 UInt256",
        "BIGINT           | Int64 Int128 Int256                     | Int8 Int16 Int32 UInt8 UInt16 UInt32 UInt64 UInt128 UInt256",
        // DECIMAL(p, s): a Decimal needs scale >= s and integer digits >= p - s; with s = 0, an Int with more
        // digits than p needs no check, one with exactly p digits or any UInt whose digits cover p is checked
        "DECIMAL(9,0)     | Int32 Int64 Int128 Int256 Decimal(18,4) | UInt32 UInt64 UInt128 UInt256",
        "DECIMAL(10,2)    | Decimal(10,2) Decimal(18,4)             | ",
        "FLOAT            | Float32 Float64                         | ",
        "DOUBLE           | Float64                                 | Float32",
        // CHAR(n)/VARCHAR(n): a FixedString(m) needs no check when 4n <= m, the most UTF-8 bytes n characters take
        "CHAR(4)          | String JSON FixedString(16)             | FixedString(4) UUID",
        "STRING           | String JSON                             | FixedString(4) FixedString(16) UUID",
        "DATE             |                                         | Date Date32",
        "TIMESTAMP(0)     |                                         | DateTime DateTime64(3) DateTime64(9)",
        "TIMESTAMP(3)     |                                         | DateTime64(3) DateTime64(9)",
        "TIMESTAMP_LTZ(3) |                                         | DateTime64(3) DateTime64(9)",
    };

    /** Every ClickHouse scalar column the matrix is checked against. */
    private static final List<String> TARGETS = List.of((
            "Bool Int8 Int16 Int32 Int64 Int128 Int256 UInt8 UInt16 UInt32 UInt64 UInt128 UInt256 "
            + "Float32 Float64 Decimal(10,2) Decimal(18,4) String FixedString(4) FixedString(16) UUID JSON "
            + "Date Date32 DateTime DateTime64(3) DateTime64(9)").split(" "));

    enum Outcome { PLANNING_CHECK, RUNTIME_CHECK, REJECTED }

    private static Stream<Arguments> matrix() {
        return Arrays.stream(MATRIX).flatMap(row -> {
            String[] cells = row.split("\\|", -1);
            String flinkType = cells[0].trim();
            Set<String> planningCheck = names(cells[1]);
            Set<String> runtimeCheck = names(cells[2]);
            return TARGETS.stream().map(target -> Arguments.of(flinkType, target,
                    planningCheck.contains(target) ? Outcome.PLANNING_CHECK
                            : runtimeCheck.contains(target) ? Outcome.RUNTIME_CHECK : Outcome.REJECTED));
        });
    }

    private static Set<String> names(String cell) {
        return cell.isBlank() ? Set.of() : Set.of(cell.trim().split("\\s+"));
    }

    private static LogicalType type(String sql) {
        return LogicalTypeParser.parse(sql + " NOT NULL", ClickHouseTypeMapperTest.class.getClassLoader());
    }

    /** A typo in the table must fail here, not pass as "rejected". */
    @Test
    void theMatrixNamesOnlyKnownColumnsAndEachOnce() {
        for (String row : MATRIX) {
            String[] cells = row.split("\\|", -1);
            Set<String> planningCheck = names(cells[1]);
            Set<String> runtimeCheck = names(cells[2]);
            assertTrue(TARGETS.containsAll(planningCheck), row);
            assertTrue(TARGETS.containsAll(runtimeCheck), row);
            assertTrue(planningCheck.stream().noneMatch(runtimeCheck::contains), row);
        }
    }

    @ParameterizedTest(name = "{0} -> {1}: {2}")
    @MethodSource("matrix")
    void everyScalarPairHasItsDocumentedOutcome(String flinkType, String target, Outcome outcome) {
        switch (outcome) {
            case PLANNING_CHECK:
                ClickHouseTypeMapper.converterFor(type(flinkType), col(target), LENIENT, "c");
                ClickHouseTypeMapper.converterFor(type(flinkType), col(target), STRICT, "c");
                break;
            case RUNTIME_CHECK:
                ClickHouseTypeMapper.converterFor(type(flinkType), col(target), LENIENT, "c");
                TypeMappingException strict = assertThrows(TypeMappingException.class,
                        () -> ClickHouseTypeMapper.converterFor(type(flinkType), col(target), STRICT, "c"));
                assertTrue(strict.getMessage().contains("'sink.strict-type-mapping'"), strict.getMessage());
                break;
            case REJECTED:
                assertThrows(TypeMappingException.class,
                        () -> ClickHouseTypeMapper.converterFor(type(flinkType), col(target), LENIENT, "c"));
                assertThrows(TypeMappingException.class,
                        () -> ClickHouseTypeMapper.converterFor(type(flinkType), col(target), STRICT, "c"));
                break;
        }
    }

    private static Stream<Arguments> acceptedCells() {
        return matrix().filter(cell -> cell.get()[2] != Outcome.REJECTED);
    }

    /** Every accepted cell converts one sample value into the Java type DataWriter's dispatch takes for the column. */
    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("acceptedCells")
    void everyAcceptedPairConvertsASampleValue(String flinkType, String target, Outcome outcome) {
        Object converted = ClickHouseTypeMapper.converterFor(type(flinkType), col(target), LENIENT, "c")
                .convert(sample(flinkType, target));
        if (converted instanceof ZonedDateTime) {
            // TIMESTAMP arrives zoned in sink.timezone, TIMESTAMP_LTZ in UTC; the instant is what the writer encodes.
            assertEquals(Instant.parse("2020-01-01T00:00:00Z"), ((ZonedDateTime) converted).toInstant());
        } else {
            assertEquals(expectedSample(flinkType, target), converted);
        }
    }

    /** 7 (or 7.5, 7.25) for numbers, abcd or a UUID for text, 2020-01-01 for dates and timestamps. */
    private static Object sample(String flinkType, String target) {
        switch (flinkType) {
            case "BOOLEAN":       return true;
            case "TINYINT":       return (byte) 7;
            case "SMALLINT":      return (short) 7;
            case "INT":           return 7;
            case "BIGINT":        return 7L;
            case "DECIMAL(9,0)":  return decimalData("7", 9, 0);
            case "DECIMAL(10,2)": return decimalData("7.25", 10, 2);
            case "FLOAT":         return 7.5f;
            case "DOUBLE":        return 7.5d;
            case "DATE":          return day(2020, 1, 1);
            case "CHAR(4)":
            case "STRING":        return str(target.equals("UUID") ? SOME_UUID.toString() : "abcd");
            default:              return ts(2020, 1, 1, 0, 0, 0);   // TIMESTAMP(p), TIMESTAMP_LTZ(p)
        }
    }

    private static Object expectedSample(String flinkType, String target) {
        switch (target) {
            case "Bool":    return true;
            case "Int8":    return (byte) 7;
            case "Int16":   return (short) 7;
            case "Int32": case "UInt8": case "UInt16":  return 7;
            case "Int64": case "UInt32":                return 7L;
            case "Int128": case "Int256": case "UInt64": case "UInt128": case "UInt256":
                            return BigInteger.valueOf(7);
            case "Float32": return 7.5f;
            case "Float64": return 7.5d;
            case "Decimal(10,2)": case "Decimal(18,4)":
                            return new BigDecimal(flinkType.equals("DECIMAL(10,2)") ? "7.25" : "7");
            case "UUID":    return SOME_UUID;
            case "Date": case "Date32":
                            return LocalDate.of(2020, 1, 1);
            default:        return "abcd";   // String, JSON, FixedString
        }
    }

    /** Runtime-check pairs: the column's bounds convert, one step beyond fails naming the column and the range. */
    private static Stream<Arguments> runtimeCheckProbes() {
        return Stream.of(
                probe("TINYINT", "UInt8", in((byte) 0, 0, (byte) 127, 127), out((byte) -1, "UInt8 range 0..255")),
                probe("SMALLINT", "Int8", in((short) -128, (byte) -128, (short) 127, (byte) 127),
                        out((short) -129, "Int8 range -128..127", (short) 128, "Int8 range -128..127")),
                probe("SMALLINT", "UInt8", in((short) 0, 0, (short) 255, 255),
                        out((short) -1, "UInt8 range 0..255", (short) 256, "UInt8 range 0..255")),
                probe("SMALLINT", "UInt16", in((short) 32767, 32767), out((short) -1, "UInt16 range 0..65535")),
                probe("INT", "Int16", in(-32768, (short) -32768, 32767, (short) 32767),
                        out(-32769, "Int16 range -32768..32767", 32768, "Int16 range -32768..32767")),
                probe("INT", "UInt16", in(0, 0, 65535, 65535), out(-1, "UInt16 range 0..65535", 65536, "UInt16 range 0..65535")),
                probe("INT", "UInt32", in(Integer.MAX_VALUE, (long) Integer.MAX_VALUE), out(-1, "UInt32 range 0..4294967295")),
                probe("BIGINT", "Int32", in((long) Integer.MIN_VALUE, Integer.MIN_VALUE, (long) Integer.MAX_VALUE, Integer.MAX_VALUE),
                        out(Integer.MIN_VALUE - 1L, "Int32 range -2147483648..2147483647", Integer.MAX_VALUE + 1L, "Int32 range -2147483648..2147483647")),
                probe("BIGINT", "UInt32", in(0L, 0L, 4294967295L, 4294967295L),
                        out(-1L, "UInt32 range 0..4294967295", 4294967296L, "UInt32 range 0..4294967295")),
                probe("BIGINT", "UInt64", in(0L, BigInteger.ZERO, Long.MAX_VALUE, BigInteger.valueOf(Long.MAX_VALUE)),
                        out(-1L, "UInt64 range 0..18446744073709551615")),
                probe("BIGINT", "UInt256", in(Long.MAX_VALUE, BigInteger.valueOf(Long.MAX_VALUE)), out(-1L, "UInt256 range 0..")),
                probe("DOUBLE", "Float32", in(1.5d, 1.5f, (double) -Float.MAX_VALUE, -Float.MAX_VALUE, Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY),
                        out(1e300, "Float32 range", -1e300, "Float32 range")),
                // 20 digits pass the planning precision check but reach past UInt64's maximum
                probe("DECIMAL(20,0)", "UInt64", in(decimal("18446744073709551615"), new BigInteger("18446744073709551615")),
                        out(decimal("99999999999999999999"), "UInt64 range", decimal("-1"), "unsigned type UInt64")),
                probe("DECIMAL(19,0)", "Int64", in(decimal("9223372036854775807"), Long.MAX_VALUE, decimal("-9223372036854775808"), Long.MIN_VALUE),
                        out(decimal("9223372036854775808"), "Int64 range -9223372036854775808..9223372036854775807")),
                probe("DECIMAL(3,0)", "Int8", in(decimal("-128"), (byte) -128), out(decimal("128"), "Int8 range -128..127")),
                probe("DECIMAL(3,0)", "UInt8", in(decimal("255"), 255),
                        out(decimal("-1"), "unsigned type UInt8", decimal("256"), "UInt8 range 0..255")),
                // three characters but six UTF-8 bytes: the limit is bytes
                probe("STRING", "FixedString(4)", in(str("abcd"), "abcd", str("ab"), "ab"), out(str("ééé"), "FixedString(4)")),
                // UUID.fromString would zero-expand 1-1-1-1-1 silently; only canonical text passes, in either case
                probe("STRING", "UUID", in(str(SOME_UUID.toString()), SOME_UUID, str(SOME_UUID.toString().toUpperCase()), SOME_UUID),
                        out(str("1-1-1-1-1"), "not a valid UUID")),
                probe("DATE", "Date", in(day(1970, 1, 1), LocalDate.of(1970, 1, 1), day(2149, 6, 6), LocalDate.of(2149, 6, 6)),
                        out(day(1969, 12, 31), "Date range", day(2149, 6, 7), "Date range")),
                probe("DATE", "Date32", in(day(1900, 1, 1), LocalDate.of(1900, 1, 1), day(2299, 12, 31), LocalDate.of(2299, 12, 31)),
                        out(day(1899, 12, 31), "Date32 range", day(9999, 12, 31), "Date32 range 1900-01-01..2299-12-31")),
                probe("TIMESTAMP(0)", "DateTime", in(ts(1970, 1, 1, 0, 0, 0), null, ts(2106, 2, 7, 6, 28, 15), null),
                        out(ts(1969, 12, 31, 23, 0, 0), "DateTime range", ts(2106, 2, 7, 6, 28, 16), "DateTime range")),
                probe("TIMESTAMP(3)", "DateTime64(3)", in(ts(1900, 1, 1, 0, 0, 0), null, ts(2299, 12, 31, 23, 59, 59), null),
                        out(ts(1899, 12, 31, 23, 59, 0), "DateTime64 range", ts(9999, 12, 31, 0, 0, 0), "DateTime64 range")),
                // inside the documented 2299 bound, but scale-9 ticks overflow Int64 after 2262-04-11
                probe("TIMESTAMP(9)", "DateTime64(9)", in(ts(2262, 4, 11, 0, 0, 0), null),
                        out(ts(2263, 1, 1, 0, 0, 0), "DateTime64 range")));
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("runtimeCheckProbes")
    void runtimeCheckPairsAcceptTheirBoundsAndRejectJustBeyond(String flinkType, String target,
                                                               Object[][] inRange, Object[][] outOfRange) {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(type(flinkType), col(target), LENIENT, "c");
        for (Object[] inputAndExpected : inRange) {
            Object converted = converter.convert(inputAndExpected[0]);
            if (inputAndExpected[1] != null) {
                assertEquals(inputAndExpected[1], converted, target + " <- " + inputAndExpected[0]);
            }
        }
        for (Object[] inputAndFragment : outOfRange) {
            assertRangeError(() -> converter.convert(inputAndFragment[0]), (String) inputAndFragment[1]);
        }
    }

    private static final UUID SOME_UUID = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");

    private static Arguments probe(String flinkType, String target, Object[][] inRange, Object[][] outOfRange) {
        return Arguments.of(flinkType, target, inRange, outOfRange);
    }

    /** (input, expected) pairs; a null expected only asserts the value converts. */
    private static Object[][] in(Object... inputAndExpected) {
        return pairs(inputAndExpected);
    }

    /** (input, message fragment) pairs. */
    private static Object[][] out(Object... inputAndFragment) {
        return pairs(inputAndFragment);
    }

    private static Object[][] pairs(Object[] flat) {
        Object[][] pairs = new Object[flat.length / 2][];
        for (int i = 0; i < pairs.length; i++) {
            pairs[i] = new Object[]{flat[2 * i], flat[2 * i + 1]};
        }
        return pairs;
    }

    private static DecimalData decimalData(String text, int precision, int scale) {
        return DecimalData.fromBigDecimal(new BigDecimal(text), precision, scale);
    }

    private static StringData str(String text) {
        return StringData.fromString(text);
    }

    private static int day(int year, int month, int dayOfMonth) {
        return (int) LocalDate.of(year, month, dayOfMonth).toEpochDay();
    }

    private static TimestampData ts(int year, int month, int day, int hour, int minute, int second) {
        return TimestampData.fromLocalDateTime(LocalDateTime.of(year, month, day, hour, minute, second));
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
    void enumTargetIsExplicitlyUnsupported() {
        TypeMappingException e = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(
                        new VarCharType(false, VarCharType.MAX_LENGTH),
                        col("Enum8('new' = 1, 'done' = 2)"), LENIENT, "c"));
        assertEquals(TypeMappingException.Kind.TARGET_UNSUPPORTED, e.getKind());
        assertTrue(e.getMessage().contains("issue #43"), e.getMessage());
    }

    @Test
    void timestampPrecisionMayNotExceedColumnScale() {
        TypeMappingException e = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(
                        new TimestampType(false, 9), col("DateTime64(3)"), LENIENT, "c"));
        assertEquals("precision 9 exceeds the column's scale 3", e.getMessage());
    }

    @Test
    void timestampIsInterpretedInTheSinkTimezone() {
        ZoneId tokyo = ZoneId.of("Asia/Tokyo");
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                new TimestampType(false, 3), col("DateTime64(3)"), lenientIn(tokyo), "c");
        LocalDateTime wallClock = LocalDateTime.of(2026, 1, 2, 3, 4, 5, 678_000_000);
        assertEquals(ZonedDateTime.of(wallClock, tokyo),
                converter.convert(TimestampData.fromLocalDateTime(wallClock)));
    }

    @Test
    void dstGapAndOverlapResolveAsDocumented() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                new TimestampType(false, 3), col("DateTime64(3)"),
                lenientIn(ZoneId.of("America/New_York")), "c");
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
                new IntType(false), col("SimpleAggregateFunction(max, Int32)"), LENIENT, "c");
        assertEquals(41, converter.convert(41));
    }

    /** Nested SAF has no wire encoding — it must be rejected at planning, never unwrapped. */
    @Test
    void nestedSimpleAggregateFunctionIsRejectedAtPlanning() {
        TypeMappingException e = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(
                        new ArrayType(false, new IntType(false)),
                        col("Array(SimpleAggregateFunction(max, Int32))"), LENIENT, "c"));
        assertEquals(TypeMappingException.Kind.TARGET_UNSUPPORTED, e.getKind());
        assertTrue(e.getMessage().contains("top-level column"), e.getMessage());
    }

    @Test
    void multisetWritesElementCountsAsLongs() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                multisetOfString(), col("Map(String, UInt64)"), LENIENT, "c");
        Map<Object, Object> counts = new LinkedHashMap<>();
        counts.put(StringData.fromString("a"), 2);
        assertEquals(Map.of("a", 2L), converter.convert(new GenericMapData(counts)));
    }

    @Test
    void multisetRequiresUInt64CountColumns() {
        TypeMappingException e = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(
                        multisetOfString(), col("Map(String, UInt32)"), LENIENT, "c"));
        assertTrue(e.getMessage().contains("exactly UInt64"), e.getMessage());
    }

    @Test
    void decimalsWiderThanTheIntegerAreRejectedAtPlanning() {
        TypeMappingException e = assertThrows(TypeMappingException.class, () -> decimalTo(20, "Int64"));
        assertTrue(e.getMessage().contains("precision 20 exceeds Int64's 19 digits"), e.getMessage());
        e = assertThrows(TypeMappingException.class, () -> decimalTo(4, "UInt8"));
        assertTrue(e.getMessage().contains("precision 4 exceeds UInt8's 3 digits"), e.getMessage());
        e = assertThrows(TypeMappingException.class, () -> decimalTo(21, "UInt64"));
        assertTrue(e.getMessage().contains("precision 21 exceeds UInt64's 20 digits"), e.getMessage());
    }

    @Test
    void nestedUnsignedValuesAreRangeCheckedToo() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                new ArrayType(false, new BigIntType(false)), col("Array(UInt32)"), LENIENT, "c");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> converter.convert(new GenericArrayData(new long[]{1L, -1L})));
        assertTrue(e.getMessage().contains("Column 'c element'"), e.getMessage());
    }

    @Test
    void rowWritesToTupleWithPositionalFields() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                rowOf(new IntType(false), new VarCharType(false, VarCharType.MAX_LENGTH)),
                col("Tuple(Int32, String)"), LENIENT, "c");
        Object[] tuple = (Object[]) converter.convert(
                GenericRowData.of(7, StringData.fromString("x")));
        assertArrayEquals(new Object[]{7, "x"}, tuple);
    }

    @Test
    void rowFieldCountMustMatchTupleElementCount() {
        TypeMappingException e = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(
                        rowOf(new IntType(false)), col("Tuple(Int32, String)"), LENIENT, "c"));
        assertEquals("ROW has 1 fields but the Tuple has 2 elements", e.getMessage());
    }

    @Test
    void nullableTupleElementsAreRejectedOnEitherSide() {
        TypeMappingException flinkSide = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(
                        rowOf(new IntType(true)), col("Tuple(Int32)"), LENIENT, "c"));
        assertTrue(flinkSide.getMessage().contains("the Flink ROW field 'f0' is nullable"),
                flinkSide.getMessage());
        assertTrue(flinkSide.getMessage().contains("declare it NOT NULL"), flinkSide.getMessage());

        TypeMappingException clickHouseSide = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(
                        rowOf(new IntType(false)), col("Tuple(Nullable(Int32))"), LENIENT, "c"));
        assertTrue(clickHouseSide.getMessage().contains("Nullable Tuple elements (Nullable(Int32) at position 1)"),
                clickHouseSide.getMessage());
    }

    @Test
    void nullableMapValuesAreRejectedOnEitherSide() {
        VarCharType key = new VarCharType(false, VarCharType.MAX_LENGTH);
        TypeMappingException flinkSide = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(
                        new MapType(false, key, new IntType(true)), col("Map(String, Int32)"), LENIENT, "c"));
        assertTrue(flinkSide.getMessage().contains("the Flink map value type INT is nullable"),
                flinkSide.getMessage());
        assertTrue(flinkSide.getMessage().contains("declare it NOT NULL"), flinkSide.getMessage());

        TypeMappingException clickHouseSide = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(
                        new MapType(false, key, new IntType(false)), col("Map(String, Nullable(Int32))"), LENIENT, "c"));
        assertTrue(clickHouseSide.getMessage().contains("Nullable Map values (Nullable(Int32))"),
                clickHouseSide.getMessage());
    }

    /** Binding is positional; the same names in another order is almost certainly a mistake. */
    @Test
    void rowFieldNamesMatchingANamedTupleInAnotherOrderAreRejected() {
        RowType swapped = new RowType(false, List.of(
                new RowType.RowField("lon", new DoubleType(false)),
                new RowType.RowField("lat", new DoubleType(false))));
        TypeMappingException e = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(swapped, col("Tuple(lat Float64, lon Float64)"), LENIENT, "c"));
        assertTrue(e.getMessage().contains("[lon, lat]"), e.getMessage());
        assertTrue(e.getMessage().contains("[lat, lon]"), e.getMessage());

        // The same order, different names, or unnamed elements keep the positional contract.
        ClickHouseTypeMapper.converterFor(swapped, col("Tuple(lon Float64, lat Float64)"), LENIENT, "c");
        ClickHouseTypeMapper.converterFor(swapped, col("Tuple(x Float64, y Float64)"), LENIENT, "c");
        ClickHouseTypeMapper.converterFor(swapped, col("Tuple(Float64, Float64)"), LENIENT, "c");
    }

    @Test
    void nullRowFieldFailsNamingTheColumn() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                rowOf(new IntType(false), new VarCharType(false, VarCharType.MAX_LENGTH)),
                col("Tuple(Int32, String)"), LENIENT, "c");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> converter.convert(GenericRowData.of(7, null)));
        assertTrue(e.getMessage().contains("Column 'c'"), e.getMessage());
        assertTrue(e.getMessage().contains("null ROW field 2"), e.getMessage());
        IllegalArgumentException first = assertThrows(IllegalArgumentException.class,
                () -> converter.convert(GenericRowData.of(null, StringData.fromString("x"))));
        assertTrue(first.getMessage().contains("null ROW field 1"), first.getMessage());
    }

    /** Flink's NOT NULL getters skip isNullAt, so a binary row's null slot would read back as 0. */
    @Test
    void nullRowFieldInABinaryRowFailsInsteadOfWritingZero() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                rowOf(new IntType(false), new IntType(false)), col("Tuple(Int32, Int32)"), LENIENT, "c");
        int size = BinaryRowData.calculateFixPartSizeInBytes(2);
        BinaryRowData row = new BinaryRowData(2);
        row.pointTo(MemorySegmentFactory.wrap(new byte[size]), 0, size);
        row.setNullAt(0);
        row.setInt(1, 5);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> converter.convert(row));
        assertTrue(e.getMessage().contains("Column 'c'"), e.getMessage());
        assertTrue(e.getMessage().contains("null ROW field 1"), e.getMessage());
    }

    @Test
    void nullArrayElementFailsForNonNullableElementsNamingTheColumn() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                new ArrayType(false, new IntType(false)), col("Array(Int32)"), LENIENT, "c");
        BinaryArrayData binary = BinaryArrayData.fromPrimitiveArray(new int[]{1, 2});
        binary.setNullInt(0);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> converter.convert(binary));
        assertTrue(e.getMessage().contains("Column 'c'"), e.getMessage());
        assertTrue(e.getMessage().contains("null array element 1"), e.getMessage());
        assertThrows(IllegalArgumentException.class,
                () -> converter.convert(new GenericArrayData(new Object[]{null, 2})));
    }

    @Test
    void nullArrayElementsAreForwardedIntoNullableElements() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                new ArrayType(false, new IntType(true)), col("Array(Nullable(Int32))"), LENIENT, "c");
        BinaryArrayData binary = BinaryArrayData.fromPrimitiveArray(new int[]{1, 2});
        binary.setNullInt(0);
        assertEquals(Arrays.asList(null, 2), converter.convert(binary));
    }

    @Test
    void nullMapValueInABinaryMapFailsNamingTheColumn() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                new MapType(false, new IntType(false), new IntType(false)), col("Map(Int32, Int32)"), LENIENT, "c");
        BinaryArrayData values = BinaryArrayData.fromPrimitiveArray(new int[]{9});
        values.setNullInt(0);
        BinaryMapData map = BinaryMapData.valueOf(BinaryArrayData.fromPrimitiveArray(new int[]{7}), values);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> converter.convert(map));
        assertTrue(e.getMessage().contains("Column 'c'"), e.getMessage());
        assertTrue(e.getMessage().contains("null map value"), e.getMessage());
    }

    /** Nullable(Array(...)) is invalid in ClickHouse, so the hint must not suggest it for composite elements. */
    @Test
    void nullableElementHintIsDroppedForCompositeElements() {
        TypeMappingException scalar = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(
                        new ArrayType(false, new IntType(true)), col("Array(Int32)"), LENIENT, "c"));
        assertTrue(scalar.getMessage().contains("or make the element Nullable"), scalar.getMessage());
        TypeMappingException composite = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(
                        new ArrayType(false, new ArrayType(true, new IntType(false))),
                        col("Array(Array(Int32))"), LENIENT, "c"));
        assertTrue(composite.getMessage().contains("declare the element NOT NULL"), composite.getMessage());
        assertFalse(composite.getMessage().contains("make the element Nullable"), composite.getMessage());
    }

    @Test
    void rowFieldRangeChecksNameTheFieldPath() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                rowOf(new SmallIntType(false)), col("Tuple(UInt8)"), LENIENT, "c");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> converter.convert(GenericRowData.of((short) 256)));
        assertTrue(e.getMessage().contains("Column 'c.f0'"), e.getMessage());
        assertTrue(e.getMessage().contains("UInt8 range 0..255"), e.getMessage());
    }

    @Test
    void multisetRejectsNegativeCounts() {
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                multisetOfString(), col("Map(String, UInt64)"), LENIENT, "c");
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
                () -> ClickHouseTypeMapper.converterFor(mapType, col("Map(UInt64, String)"), LENIENT, "c"));
        assertTrue(e.getMessage().contains("Map keys of type UInt64"), e.getMessage());
        assertTrue(e.getMessage().contains("upper half of the UInt64 range"), e.getMessage());

        TypeMappingException multisetError = assertThrows(TypeMappingException.class,
                () -> ClickHouseTypeMapper.converterFor(
                        new MultisetType(false, new DecimalType(false, 20, 0)),
                        col("Map(UInt64, UInt64)"), LENIENT, "c"));
        assertTrue(multisetError.getMessage().contains("Map keys of type UInt64"),
                multisetError.getMessage());
    }

    @Test
    void uInt128MapKeysStayAccepted() {
        // UInt128 keys use client-v2's BigInteger parse — the full range round-trips.
        MapType mapType = new MapType(false,
                new DecimalType(false, 38, 0), new VarCharType(false, VarCharType.MAX_LENGTH));
        ValueConverter converter = ClickHouseTypeMapper.converterFor(
                mapType, col("Map(UInt128, String)"), LENIENT, "c");
        Map<Object, Object> entries = new LinkedHashMap<>();
        entries.put(DecimalData.fromBigDecimal(new BigDecimal("18446744073709551616"), 38, 0),
                StringData.fromString("v"));
        assertEquals(Map.of("18446744073709551616", "v"),
                converter.convert(new GenericMapData(entries)));
    }

    @Test
    void decimalMapKeysAreAccepted() {
        // client-v2 restores Decimal map keys with new BigDecimal(String), so every width round-trips.
        MapType mapType = new MapType(false,
                new DecimalType(false, 9, 2), new VarCharType(false, VarCharType.MAX_LENGTH));
        for (String key : List.of("Decimal(10, 2)", "Decimal32(2)", "Decimal64(2)", "Decimal128(2)", "Decimal256(2)")) {
            ValueConverter converter = ClickHouseTypeMapper.converterFor(
                    mapType, col("Map(" + key + ", String)"), LENIENT, "c");
            Map<Object, Object> entries = new LinkedHashMap<>();
            entries.put(DecimalData.fromBigDecimal(new BigDecimal("12.34"), 9, 2), StringData.fromString("v"));
            assertEquals(Map.of("12.34", "v"), converter.convert(new GenericMapData(entries)), key);
        }
    }

    /** Composite nesting: ROW inside ARRAY, MAP and ROW all reach the writer as Object[] tuples. */
    @Test
    void rowsNestedInArraysMapsAndRowsWriteTuples() {
        RowType pair = rowOf(new IntType(false), new VarCharType(false, VarCharType.MAX_LENGTH));

        ValueConverter arrayConverter = ClickHouseTypeMapper.converterFor(
                new ArrayType(false, pair), col("Array(Tuple(Int32, String))"), LENIENT, "c");
        List<?> tuples = (List<?>) arrayConverter.convert(new GenericArrayData(new Object[]{
                GenericRowData.of(1, StringData.fromString("p")),
                GenericRowData.of(2, StringData.fromString("q"))}));
        assertEquals(2, tuples.size());
        assertArrayEquals(new Object[]{1, "p"}, (Object[]) tuples.get(0));
        assertArrayEquals(new Object[]{2, "q"}, (Object[]) tuples.get(1));

        ValueConverter mapConverter = ClickHouseTypeMapper.converterFor(
                new MapType(false, new VarCharType(false, VarCharType.MAX_LENGTH), pair),
                col("Map(String, Tuple(Int32, String))"), LENIENT, "c");
        Map<Object, Object> entries = new LinkedHashMap<>();
        entries.put(StringData.fromString("k"), GenericRowData.of(1, StringData.fromString("p")));
        Map<?, ?> payload = (Map<?, ?>) mapConverter.convert(new GenericMapData(entries));
        assertArrayEquals(new Object[]{1, "p"}, (Object[]) payload.get("k"));

        ValueConverter nestedConverter = ClickHouseTypeMapper.converterFor(
                rowOf(new IntType(false), pair), col("Tuple(Int32, Tuple(Int32, String))"), LENIENT, "c");
        Object[] outer = (Object[]) nestedConverter.convert(
                GenericRowData.of(5, GenericRowData.of(6, StringData.fromString("z"))));
        assertEquals(5, outer[0]);
        assertArrayEquals(new Object[]{6, "z"}, (Object[]) outer[1]);
    }

    private static void assertRangeError(Executable call, String expectedFragment) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, call);
        assertTrue(e.getMessage().contains("Column 'c'"), e.getMessage());
        assertTrue(e.getMessage().contains(expectedFragment), e.getMessage());
    }

    private static ValueConverter decimalTo(int precision, String target) {
        return ClickHouseTypeMapper.converterFor(new DecimalType(false, precision, 0), col(target), LENIENT, "c");
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
