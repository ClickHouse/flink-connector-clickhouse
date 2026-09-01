package com.clickhouse.utils.writer;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataWriterDispatchTest {

    private byte[] serialize(Object value, ClickHouseColumn col) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataWriter dw = DataWriter.of(out);
        dw.writeValue(value, col);
        return out.toByteArray();
    }

    @Test void dispatchInt8() throws IOException {
        byte[] r = serialize((byte) 42, ClickHouseColumn.of("c", "Int8"));
        assertTrue(r.length >= 1);
    }

    @Test void dispatchInt32() throws IOException {
        byte[] r = serialize(123456, ClickHouseColumn.of("c", "Int32"));
        assertTrue(r.length >= 4);
    }

    @Test void dispatchInt64() throws IOException {
        byte[] r = serialize(1234567890123L, ClickHouseColumn.of("c", "Int64"));
        assertTrue(r.length >= 8);
    }

    @Test void dispatchUInt8FromInteger() throws IOException {
        byte[] r = serialize(200, ClickHouseColumn.of("c", "UInt8"));
        assertTrue(r.length >= 1);
    }

    @Test void dispatchUInt64FromBigInteger() throws IOException {
        byte[] r = serialize(new BigInteger("18446744073709551615"), ClickHouseColumn.of("c", "UInt64"));
        assertTrue(r.length >= 8);
    }

    @Test void dispatchDecimal() throws IOException {
        byte[] r = serialize(new BigDecimal("123.45"), ClickHouseColumn.of("c", "Decimal(10,2)"));
        assertTrue(r.length > 0);
    }

    @Test void dispatchString() throws IOException {
        byte[] r = serialize("hello", ClickHouseColumn.of("c", "String"));
        assertTrue(r.length > 0);
    }

    @Test void dispatchFixedString() throws IOException {
        byte[] r = serialize("12345", ClickHouseColumn.of("c", "FixedString(5)"));
        assertTrue(r.length >= 5);
    }

    @Test void dispatchNullableFixedStringWritesFullWidth() throws IOException {
        // Sizing must come from getPrecision(): getEstimatedLength() is 1 for Nullable(FixedString(n)).
        byte[] r = serialize("12345", ClickHouseColumn.of("c", "Nullable(FixedString(5))"));
        assertArrayEquals(new byte[]{0, '1', '2', '3', '4', '5'}, r);
    }

    @Test void dispatchUUID() throws IOException {
        byte[] r = serialize(UUID.randomUUID(), ClickHouseColumn.of("c", "UUID"));
        assertTrue(r.length >= 16);
    }

    @Test void dispatchDateFromLocalDate() throws IOException {
        byte[] r = serialize(LocalDate.of(2026, 5, 16), ClickHouseColumn.of("c", "Date"));
        assertTrue(r.length > 0);
    }

    @Test void dispatchDateFromZonedDateTime() throws IOException {
        byte[] r = serialize(ZonedDateTime.now(ZoneId.of("UTC")), ClickHouseColumn.of("c", "Date"));
        assertTrue(r.length > 0);
    }

    @Test void dispatchDateTime64WithScale() throws IOException {
        byte[] r = serialize(LocalDateTime.now(), ClickHouseColumn.of("c", "DateTime64(6)"));
        assertTrue(r.length > 0);
    }

    @Test void dispatchArray() throws IOException {
        ClickHouseColumn col = ClickHouseColumn.of("c", ClickHouseDataType.Array, false,
                ClickHouseColumn.of("", "String"));
        byte[] r = serialize(Arrays.asList("a", "b"), col);
        assertTrue(r.length > 0);
    }

    // Object[] is the tuple shape the Table API mapper produces (toPayloadTuple).
    @Test void dispatchTupleFromObjectArray() throws IOException {
        byte[] r = serialize(new Object[]{42, "ab"}, ClickHouseColumn.of("c", "Tuple(Int32, String)"));
        assertArrayEquals(new byte[]{42, 0, 0, 0, 2, 'a', 'b'}, r);
    }

    @Test void dispatchArrayNullableElementWritesNullMarker() throws IOException {
        byte[] r = serialize(Arrays.asList(7, null), ClickHouseColumn.of("c", "Array(Nullable(Int32))"));
        // varint count, then per element: null marker byte (+ value when non-null).
        assertArrayEquals(new byte[]{2, 0, 7, 0, 0, 0, 1}, r);
    }

    @Test void dispatchBoolean() throws IOException {
        byte[] r = serialize(true, ClickHouseColumn.of("c", "Bool"));
        assertTrue(r.length >= 1);
    }

    // SimpleAggregateFunction serializes as its inner type (wire encoding == inner type; only the header differs). See issue #143.
    @Test void dispatchSimpleAggregateFunctionString() throws IOException {
        byte[] r = serialize("abc",
                ClickHouseColumn.of("c", "SimpleAggregateFunction(max, String)"));
        assertTrue(r.length > 0);
    }

    @Test void dispatchSimpleAggregateFunctionInt64() throws IOException {
        byte[] r = serialize(1234567890123L,
                ClickHouseColumn.of("c", "SimpleAggregateFunction(sum, Int64)"));
        assertTrue(r.length >= 8);
    }

    @Test void simpleAggregateFunctionMatchesInnerTypeEncoding() throws IOException {
        byte[] agg = serialize("abc",
                ClickHouseColumn.of("c", "SimpleAggregateFunction(max, String)"));
        byte[] plain = serialize("abc", ClickHouseColumn.of("c", "String"));
        assertArrayEquals(plain, agg);
    }

    // LowCardinality inner: getDataType() collapses to String; the SAF wrapper must too.
    @Test void simpleAggregateFunctionLowCardinalityMatchesInner() throws IOException {
        byte[] agg = serialize("abc",
                ClickHouseColumn.of("c", "SimpleAggregateFunction(max, LowCardinality(String))"));
        byte[] inner = serialize("abc", ClickHouseColumn.of("c", "LowCardinality(String)"));
        assertArrayEquals(inner, agg);
    }

    // Nullable inner, non-null value: nullability must come from the nested column.
    @Test void simpleAggregateFunctionNullableNonNullMatchesInner() throws IOException {
        byte[] agg = serialize("abc",
                ClickHouseColumn.of("c", "SimpleAggregateFunction(anyLast, Nullable(String))"));
        byte[] inner = serialize("abc", ClickHouseColumn.of("c", "Nullable(String)"));
        assertArrayEquals(inner, agg);
    }

    // Nullable inner, null value: must write the null marker (nullability from the nested column), not throw.
    @Test void simpleAggregateFunctionNullableNullMatchesInner() throws IOException {
        byte[] agg = serialize(null,
                ClickHouseColumn.of("c", "SimpleAggregateFunction(anyLast, Nullable(String))"));
        byte[] inner = serialize(null, ClickHouseColumn.of("c", "Nullable(String)"));
        assertArrayEquals(inner, agg);
        assertTrue(agg.length >= 1);
    }

    // Decimal inner: precision/scale must be read from the nested column, not the outer one.
    @Test void simpleAggregateFunctionDecimalMatchesInner() throws IOException {
        byte[] agg = serialize(new BigDecimal("123.4567"),
                ClickHouseColumn.of("c", "SimpleAggregateFunction(sum, Decimal(18, 4))"));
        byte[] inner = serialize(new BigDecimal("123.4567"), ClickHouseColumn.of("c", "Decimal(18, 4)"));
        assertArrayEquals(inner, agg);
    }

    /** Inner types carrying detail (length, scale, element types) the outer column does not report. */
    private static Stream<Arguments> wrappedInnerTypes() {
        return Stream.of(
            Arguments.of("FixedString(4)", "anyLast", "abcd"),
            Arguments.of("DateTime64(3)", "anyLast", LocalDateTime.of(2024, 3, 10, 8, 45, 12, 123_000_000)),
            Arguments.of("LowCardinality(Nullable(String))", "anyLast", "lc"),
            Arguments.of("Array(String)", "groupArrayArray", List.of("a", "b")),
            Arguments.of("Map(String, UInt64)", "sumMap", Map.of("k", 7L)),
            Arguments.of("Tuple(Int32, String)", "anyLast", List.of(1, "t"))
        );
    }

    @ParameterizedTest(name = "SimpleAggregateFunction({1}, {0})")
    @MethodSource("wrappedInnerTypes")
    void wrappedInnerTypeMatchesPlainInnerType(String innerType, String function, Object value)
            throws IOException {
        byte[] agg = serialize(value,
                ClickHouseColumn.of("c", "SimpleAggregateFunction(" + function + ", " + innerType + ")"));
        byte[] inner = serialize(value, ClickHouseColumn.of("c", innerType));
        assertArrayEquals(inner, agg);
        assertTrue(agg.length > 0);
    }
}
