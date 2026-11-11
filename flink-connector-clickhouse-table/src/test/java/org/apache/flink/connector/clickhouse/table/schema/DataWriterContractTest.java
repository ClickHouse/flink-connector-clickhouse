package org.apache.flink.connector.clickhouse.table.schema;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.format.BinaryStreamUtils;
import com.clickhouse.utils.writer.DataWriter;

import org.apache.flink.connector.clickhouse.data.TypeTags;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Pins the planning-time lists in {@link ClickHouseTypeMapper} and {@link SchemaResolver} to DataWriter's actual behavior. */
class DataWriterContractTest {

    /** A sample column expression per writable type; extend when WRITABLE_TARGETS grows. */
    private static final Map<ClickHouseDataType, String> TYPE_EXPRESSIONS = buildTypeExpressions();

    private static Map<ClickHouseDataType, String> buildTypeExpressions() {
        Map<ClickHouseDataType, String> m = new EnumMap<>(ClickHouseDataType.class);
        m.put(ClickHouseDataType.Bool, "Bool");
        m.put(ClickHouseDataType.Int8, "Int8");
        m.put(ClickHouseDataType.Int16, "Int16");
        m.put(ClickHouseDataType.Int32, "Int32");
        m.put(ClickHouseDataType.Int64, "Int64");
        m.put(ClickHouseDataType.Int128, "Int128");
        m.put(ClickHouseDataType.Int256, "Int256");
        m.put(ClickHouseDataType.UInt8, "UInt8");
        m.put(ClickHouseDataType.UInt16, "UInt16");
        m.put(ClickHouseDataType.UInt32, "UInt32");
        m.put(ClickHouseDataType.UInt64, "UInt64");
        m.put(ClickHouseDataType.UInt128, "UInt128");
        m.put(ClickHouseDataType.UInt256, "UInt256");
        m.put(ClickHouseDataType.Float32, "Float32");
        m.put(ClickHouseDataType.Float64, "Float64");
        m.put(ClickHouseDataType.Decimal, "Decimal(10, 2)");
        m.put(ClickHouseDataType.Decimal32, "Decimal32(2)");
        m.put(ClickHouseDataType.Decimal64, "Decimal64(2)");
        m.put(ClickHouseDataType.Decimal128, "Decimal128(2)");
        m.put(ClickHouseDataType.Decimal256, "Decimal256(2)");
        m.put(ClickHouseDataType.String, "String");
        m.put(ClickHouseDataType.FixedString, "FixedString(4)");
        m.put(ClickHouseDataType.UUID, "UUID");
        m.put(ClickHouseDataType.JSON, "JSON");
        m.put(ClickHouseDataType.Date, "Date");
        m.put(ClickHouseDataType.Date32, "Date32");
        m.put(ClickHouseDataType.DateTime, "DateTime");
        m.put(ClickHouseDataType.DateTime64, "DateTime64(3)");
        m.put(ClickHouseDataType.Array, "Array(Int32)");
        m.put(ClickHouseDataType.Map, "Map(String, Int32)");
        m.put(ClickHouseDataType.Tuple, "Tuple(Int32, String)");
        return m;
    }

    /** Whitelist drift, direction one: planning admits a type whose every record would die. */
    @Test
    void everyWritableTargetDispatchesInDataWriter() {
        for (ClickHouseDataType type : ClickHouseTypeMapper.WRITABLE_TARGETS) {
            String expression = TYPE_EXPRESSIONS.get(type);
            assertNotNull(expression, "add a sample column expression for " + type);
            assertTrue(dispatches(expression),
                    type + " is in WRITABLE_TARGETS but DataWriter has no case for it");
        }
    }

    /** Whitelist drift, direction two: fails when DataWriter learns a type unsupportedTargetReason still cites. */
    @Test
    void knownUnwritableTargetsStayUndispatched() {
        for (String expression : List.of("Enum8('a' = 1)", "Enum16('a' = 1)", "Variant(String, Int32)", "Time", "Time64(3)")) {
            assertFalse(dispatches(expression),
                    expression + " became writable — update WRITABLE_TARGETS and unsupportedTargetReason");
        }
    }

    /**
     * Canary for {@link SchemaResolver#NULL_HANDLING_BROKEN_TARGETS} (issue #144): when an
     * assertion fails, the writer learned that null — remove the type there and here.
     */
    @Test
    void nullHandlingBrokenTargetsStayBroken() throws IOException {
        assertArrayEquals(serialize(0, column("Nullable(UInt8)")),
                serialize(null, column("Nullable(UInt8)")));
        assertArrayEquals(serialize(0, column("Nullable(UInt16)")),
                serialize(null, column("Nullable(UInt16)")));
        assertArrayEquals(serialize(0L, column("Nullable(UInt32)")),
                serialize(null, column("Nullable(UInt32)")));
        assertThrows(NullPointerException.class,
                () -> serialize(null, column("Nullable(UInt64)")));
        // The set and the assertions above must cover exactly the same types.
        assertEquals(EnumSet.of(ClickHouseDataType.UInt8, ClickHouseDataType.UInt16,
                        ClickHouseDataType.UInt32, ClickHouseDataType.UInt64),
                SchemaResolver.NULL_HANDLING_BROKEN_TARGETS);
    }

    /** The mapper spells its type ranges out; while the client still ships them, the two must agree. */
    @Test
    @SuppressWarnings("deprecation")
    void rangeBoundsMatchTheClientsConstants() {
        assertEquals(BinaryStreamUtils.U_INT8_MAX, ClickHouseTypeMapper.UINT8_MAX);
        assertEquals(BinaryStreamUtils.U_INT16_MAX, ClickHouseTypeMapper.UINT16_MAX);
        assertEquals(BinaryStreamUtils.U_INT32_MAX, ClickHouseTypeMapper.UINT32_MAX);
        assertEquals(BinaryStreamUtils.U_INT64_MAX, ClickHouseTypeMapper.UINT64_MAX);
        assertEquals(BinaryStreamUtils.U_INT16_MAX, ClickHouseTypeMapper.DATE_MAX_EPOCH_DAY);
        assertEquals(BinaryStreamUtils.DATE32_MIN, ClickHouseTypeMapper.DATE32_MIN_EPOCH_DAY);
        assertEquals(BinaryStreamUtils.DATE32_MAX, ClickHouseTypeMapper.DATE32_MAX_EPOCH_DAY);
        assertEquals(BinaryStreamUtils.U_INT32_MAX, ClickHouseTypeMapper.DATETIME_MAX_EPOCH_SECOND);
        assertEquals(BinaryStreamUtils.DATETIME64_MIN, ClickHouseTypeMapper.DATETIME64_MIN_EPOCH_SECOND);
        assertEquals(BinaryStreamUtils.DATETIME64_MAX, ClickHouseTypeMapper.DATETIME64_MAX_EPOCH_SECOND);
    }

    /** The mapper admits Nullable Map values and Tuple elements because the client writes their null marker (clickhouse-java#2886). */
    @Test
    void nestedNullableValuesCarryANullMarker() throws IOException {
        assertArrayEquals(bytes(2, 1, 'a', 0, 1, 0, 0, 0, 1, 'b', 1),
                serialize(scoresWithANull(), column("Map(String, Nullable(Int32))")));
        assertArrayEquals(bytes(1, 0, 1, 'x'),
                serialize(new Object[]{null, "x"}, column("Tuple(Nullable(Int32), Nullable(String))")));
        assertArrayEquals(bytes(1, 1, 'a', 1),
                serialize(Collections.singletonMap("a", null), column("Map(String, Nullable(Tuple(Int32)))")));
    }

    /** The mapper matches SimpleAggregateFunction inside composites on its inner type because the client writes it as that type (clickhouse-java#2937). */
    @Test
    void simpleAggregateFunctionInsideCompositesWritesAsItsInnerType() throws IOException {
        assertArrayEquals(bytes(2, 0, 41, 0, 0, 0, 1),
                serialize(Arrays.asList(41, null), column("Array(SimpleAggregateFunction(anyLast, Nullable(Int32)))")));
        assertArrayEquals(bytes(1, 3, 0, 0, 0, 0, 0, 0, 0),
                serialize(new Object[]{null, 3L}, column(
                        "Tuple(SimpleAggregateFunction(anyLast, Nullable(String)), SimpleAggregateFunction(max, Int64))")));
        assertArrayEquals(bytes(1, 1, 'a', 2, 0, 0, 0, 0, 0, 0, 0),
                serialize(Map.of("a", 2L), column("Map(String, SimpleAggregateFunction(sum, Int64))")));
    }

    /** Canary for the mapper's SimpleAggregateFunction Map-key rejection: when this fails, the client writes such keys and the rejection can go. */
    @Test
    void simpleAggregateFunctionMapKeysStayUnwritable() {
        assertThrows(UnsupportedOperationException.class,
                () -> serialize(Map.of("k", 1), column("Map(SimpleAggregateFunction(max, String), Int32)")));
    }

    /** A restored payload skips the mapper, so nested nulls must come back from the checkpoint encoding byte-identical. */
    @Test
    void nestedNullsRestoreFromACheckpointToTheSameBytes() throws IOException {
        assertRestoresToTheSameBytes(scoresWithANull(), "Map(String, Nullable(Int32))");
        assertRestoresToTheSameBytes(new Object[]{null, "x"}, "Tuple(Nullable(Int32), Nullable(String))");
        assertRestoresToTheSameBytes(Arrays.asList(41, null), "Array(SimpleAggregateFunction(anyLast, Nullable(Int32)))");
    }

    // ------------------------------------------------------------------------------------

    private static void assertRestoresToTheSameBytes(Object value, String typeExpression) throws IOException {
        ByteArrayOutputStream checkpoint = new ByteArrayOutputStream();
        TypeTags.write(value, new DataOutputStream(checkpoint));
        Object restored = TypeTags.read(
                new DataInputStream(new ByteArrayInputStream(checkpoint.toByteArray())), TypeTags.V3);
        assertArrayEquals(serialize(value, column(typeExpression)), serialize(restored, column(typeExpression)));
    }

    private static Map<String, Object> scoresWithANull() {
        Map<String, Object> scores = new LinkedHashMap<>();
        scores.put("a", 1);
        scores.put("b", null);
        return scores;
    }

    private static byte[] bytes(int... values) {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return bytes;
    }

    private static final Object PROBE = new Object();

    /** True iff the writer's dispatch has a case for the type — the probe value is never valid. */
    private static boolean dispatches(String typeExpression) {
        try {
            serialize(PROBE, column(typeExpression));
            return true;
        } catch (IOException e) {
            return !e.getMessage().startsWith("Unsupported ClickHouseDataType");
        } catch (RuntimeException e) {
            return true; // reached a typed writer (e.g. ClassCastException on the probe)
        }
    }

    private static byte[] serialize(Object value, ClickHouseColumn column) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataWriter.of(out).writeValue(value, column);
        return out.toByteArray();
    }

    private static ClickHouseColumn column(String typeExpression) {
        return ClickHouseColumn.of("c", typeExpression);
    }
}
