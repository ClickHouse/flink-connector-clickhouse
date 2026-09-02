package org.apache.flink.connector.clickhouse.table.schema;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.utils.writer.DataWriter;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.EnumMap;
import java.util.EnumSet;
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

    /** Whitelist drift, direction two: fails when DataWriter learns these (issue #43). */
    @Test
    void knownUnwritableTargetsStayUndispatched() {
        assertFalse(dispatches("Enum8('a' = 1)"), "Enum8 became writable — update WRITABLE_TARGETS");
        assertFalse(dispatches("Enum16('a' = 1)"), "Enum16 became writable — update WRITABLE_TARGETS");
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

    // ------------------------------------------------------------------------------------

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
