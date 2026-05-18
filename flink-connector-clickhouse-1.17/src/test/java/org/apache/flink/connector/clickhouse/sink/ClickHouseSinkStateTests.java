package org.apache.flink.connector.clickhouse.sink;

import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.convertor.ColumnBinding;
import org.apache.flink.connector.clickhouse.convertor.DataMapper;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;

import com.clickhouse.data.ClickHouseDataType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClickHouseSinkStateTests {

    private static BufferedRequestState<ClickHousePayload> wrap(ClickHousePayload... entries) {
        List<RequestEntryWrapper<ClickHousePayload>> list = new java.util.ArrayList<>();
        for (ClickHousePayload p : entries) {
            int size = p.getCachedBytesLength() < 0 ? 1 : p.getCachedBytesLength();
            list.add(new RequestEntryWrapper<>(p, size));
        }
        return new BufferedRequestState<>(list);
    }

    private static BufferedRequestState<ClickHousePayload> roundTrip(
            ClickHouseAsyncSinkSerializer ser, BufferedRequestState<ClickHousePayload> in) throws IOException {
        byte[] blob = ser.serialize(in);
        return ser.deserialize(ser.getVersion(), blob);
    }

    @Test void typeTagRoundTripEveryTypeOnce() throws IOException {
        ClickHousePayload p = new ClickHousePayload();
        p.getData().put("null_v", null);
        p.getData().put("bool_v", true);
        p.getData().put("byte_v", (byte) -42);
        p.getData().put("short_v", (short) 1234);
        p.getData().put("int_v", 123456789);
        p.getData().put("long_v", 1234567890123L);
        p.getData().put("float_v", 3.14f);
        p.getData().put("double_v", 2.71828d);
        p.getData().put("bigint_v", new BigInteger("123456789012345678901234567890"));
        p.getData().put("bigdec_v", new BigDecimal("1234567890.12345"));
        p.getData().put("string_v", "hello, world 🌍");
        p.getData().put("bytes_v", new byte[]{1, 2, 3, -1});
        p.getData().put("uuid_v", UUID.fromString("11111111-2222-3333-4444-555555555555"));
        p.getData().put("date_v", LocalDate.of(2026, 5, 16));
        p.getData().put("ldt_v", LocalDateTime.of(2026, 5, 16, 12, 34, 56, 789_000_000));
        p.getData().put("zdt_v", ZonedDateTime.of(2026, 5, 16, 12, 34, 56, 789_000_000,
                ZoneId.of("Europe/Berlin")));
        p.getData().put("list_v", Arrays.asList("a", 1, null));
        Map<String, Object> innerMap = new LinkedHashMap<>();
        innerMap.put("k", 42);
        p.getData().put("map_v", innerMap);
        p.getData().put("tuple_v", new Object[]{"hello", 42L, true});
        p.setCachedBytes(new byte[]{1}); // size estimate

        ClickHouseAsyncSinkSerializer ser = new ClickHouseAsyncSinkSerializer(false);
        BufferedRequestState<ClickHousePayload> restored = roundTrip(ser, wrap(p));

        assertEquals(1, restored.getBufferedRequestEntries().size());
        ClickHousePayload r = restored.getBufferedRequestEntries().iterator().next().getRequestEntry();
        assertEquals(p.getData().keySet(), r.getData().keySet());
        for (String k : p.getData().keySet()) {
            Object orig = p.getData().get(k);
            Object got  = r.getData().get(k);
            if (orig instanceof byte[]) assertArrayEquals((byte[]) orig, (byte[]) got);
            else if (orig instanceof Object[]) assertArrayEquals((Object[]) orig, (Object[]) got);
            else assertEquals(orig, got);
        }
    }

    @Test void stringModeRoundTrip() throws IOException {
        byte[] payload = "hello,world\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        ClickHousePayload p = ClickHousePayload.ofRaw(payload);

        ClickHouseAsyncSinkSerializer ser = new ClickHouseAsyncSinkSerializer(true);
        BufferedRequestState<ClickHousePayload> restored = roundTrip(ser, wrap(p));

        ClickHousePayload r = restored.getBufferedRequestEntries().iterator().next().getRequestEntry();
        assertTrue(r.isRaw());
        assertArrayEquals(payload, (byte[]) r.getData().get(ClickHousePayload.RAW_KEY));
    }

    @Test void v1LegacyRestoreInStringModeWrapsBytes() throws IOException {
        // Synthesize a V1 blob: framing identical to parent's, one ENTRY_BYTES_ONLY entry.
        byte[] legacyPayload = "csv,row\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] blob = synthesizeV1Blob(legacyPayload);

        ClickHouseAsyncSinkSerializer ser = new ClickHouseAsyncSinkSerializer(true);
        BufferedRequestState<ClickHousePayload> restored = ser.deserialize(1, blob);

        assertEquals(1, restored.getBufferedRequestEntries().size());
        ClickHousePayload r = restored.getBufferedRequestEntries().iterator().next().getRequestEntry();
        assertTrue(r.isRaw());
        assertArrayEquals(legacyPayload, (byte[]) r.getData().get(ClickHousePayload.RAW_KEY));
    }

    @Test void v1LegacyRestoreInTypedModeThrowsDrainFirst() throws IOException {
        byte[] blob = synthesizeV1Blob(new byte[]{1, 2, 3});
        ClickHouseAsyncSinkSerializer ser = new ClickHouseAsyncSinkSerializer(false);
        IOException ex = assertThrows(IOException.class, () -> ser.deserialize(1, blob));
        assertTrue(ex.getMessage().contains("Drain the previous sink before upgrading"));
    }

    @Test void unsupportedValueTypeFailsAtSerialize() {
        ClickHousePayload p = new ClickHousePayload();
        p.getData().put("bad", new java.util.Date());
        p.setCachedBytes(new byte[]{1});

        ClickHouseAsyncSinkSerializer ser = new ClickHouseAsyncSinkSerializer(false);
        IOException ex = assertThrows(IOException.class, () -> ser.serialize(wrap(p)));
        assertTrue(ex.getMessage().contains("Unsupported map value type"));
    }

    /** Framing identical to parent {@code AsyncSinkWriterStateSerializer}: DATA_IDENTIFIER, num_entries, [size][body]. */
    private static byte[] synthesizeV1Blob(byte[] entryPayload) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            out.writeLong(-1L);                          // DATA_IDENTIFIER (parent constant)
            out.writeInt(1);                             // one entry
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            try (DataOutputStream b = new DataOutputStream(body)) {
                b.writeInt(1);                           // ENTRY_BYTES_ONLY marker
                b.writeInt(entryPayload.length);
                b.write(entryPayload);
            }
            out.writeLong(body.size());                  // per-entry size prefix
            out.write(body.toByteArray());
        }
        return baos.toByteArray();
    }

    @Test void reservedKeyCollisionAtOpenThrows() {
        DataMapper<String> mapper = new DataMapper<String>() {
            @Override public void toMap(String input, Map<String, Object> map) {
                map.put(ClickHousePayload.RAW_KEY, input);
            }
            @Override public List<ColumnBinding> bindings() {
                return List.of(ColumnBinding.scalar(ClickHousePayload.RAW_KEY, "bad", ClickHouseDataType.String));
            }
        };
        ClickHouseConvertor<String> conv = new ClickHouseConvertor<>(String.class, mapper);
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> conv.open(null));
        assertTrue(ex.getMessage().contains(ClickHousePayload.RAW_KEY));
    }

    @Test void schemaEvolutionMissingKeyDeserializesNull() throws IOException {
        // Serialize a Map without key "newKey"; deserialize; verify new bindings can read null safely.
        ClickHousePayload p = new ClickHousePayload();
        p.getData().put("oldKey", "value");
        p.setCachedBytes(new byte[]{1});

        ClickHouseAsyncSinkSerializer ser = new ClickHouseAsyncSinkSerializer(false);
        BufferedRequestState<ClickHousePayload> restored = roundTrip(ser, wrap(p));
        ClickHousePayload r = restored.getBufferedRequestEntries().iterator().next().getRequestEntry();
        assertNotNull(r.getData());
        assertEquals("value", r.getData().get("oldKey"));
        assertEquals(null, r.getData().get("newKey")); // missing key returns null — user's responsibility
    }
}
