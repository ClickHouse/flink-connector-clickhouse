package org.apache.flink.connector.clickhouse.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ClickHouseSinkStateTests {

    private static ClickHouseAsyncSinkSerializer<String> stringSerializer() {
        TypeSerializer<String> ts =
                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig());
        return new ClickHouseAsyncSinkSerializer<>(ts);
    }

    @Test
    void testSerializeAndDeserializeBytesOnlyPayloadAsV1() throws Exception {
        // Bytes-only payload (data == null) takes the V1 wire path on serialize.
        byte[] data = {'H', 'e', 'l', 'l', 'o', 'W', 'o', 'r', 'l', 'd'};
        ClickHousePayload<String> payload = new ClickHousePayload<>(data);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        ClickHouseAsyncSinkSerializer<String> serializer = stringSerializer();
        serializer.serializeRequestToStream(payload, dos);

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        ClickHousePayload<String> restored = serializer.deserializeRequestFromStream(dos.size(), dis);

        Assertions.assertArrayEquals(data, restored.getCachedBytes());
        Assertions.assertNull(restored.getData(), "V1-restored entry has no typed data");
        Assertions.assertFalse(restored.needsRehydration());
    }

    @Test
    void testSerializeAndDeserializeEmptyPayloadAsV1() throws Exception {
        ClickHousePayload<String> payload = new ClickHousePayload<>((byte[]) null);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        ClickHouseAsyncSinkSerializer<String> serializer = stringSerializer();
        serializer.serializeRequestToStream(payload, dos);

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        ClickHousePayload<String> restored = serializer.deserializeRequestFromStream(dos.size(), dis);

        Assertions.assertNull(restored.getCachedBytes());
        Assertions.assertEquals(-1, restored.getCachedBytesLength());
    }

    @Test
    void testDeserializePayloadWithUnsupportedVersion() throws IOException {
        byte[] data = {'H', 'e', 'l', 'l', 'o', 'W', 'o', 'r', 'l', 'd'};
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        int V99 = 99;
        dos.writeInt(V99);
        dos.writeInt(data.length);
        dos.write(data);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));

        ClickHouseAsyncSinkSerializer<String> serializer = stringSerializer();
        Exception exception = Assertions.assertThrows(IOException.class,
                () -> serializer.deserializeRequestFromStream(dos.size(), dis));
        Assertions.assertEquals("Unsupported serialization version: 99", exception.getMessage());
    }

    @Test
    void testV2RoundTripPersistsTypedDataOnly() throws Exception {
        // When data is set, V2 path is taken — only the typed record is persisted.
        // Bytes are NOT in the wire form; on restore the entry is marked needsRehydration().
        String data = "Hello World";
        byte[] bytes = data.getBytes();
        ClickHousePayload<String> payload = new ClickHousePayload<>(bytes, data);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        ClickHouseAsyncSinkSerializer<String> serializer = stringSerializer();
        serializer.serializeRequestToStream(payload, dos);

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        ClickHousePayload<String> restored = serializer.deserializeRequestFromStream(baos.size(), dis);

        Assertions.assertNull(restored.getCachedBytes(), "V2-restored entry has no bytes yet");
        Assertions.assertTrue(restored.needsRehydration(), "V2-restored entry needs rehydration");
        Assertions.assertEquals(data, restored.getData());
    }

    @Test
    void testV1BackwardCompatibilityWithHandcraftedBytes() throws Exception {
        // Hand-write the V1 wire format exactly as old code produced it.
        byte[] data = {'T', 'e', 's', 't'};
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(1); // V1 header
        dos.writeInt(data.length);
        dos.write(data);

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        ClickHouseAsyncSinkSerializer<String> serializer = stringSerializer();
        ClickHousePayload<String> restored = serializer.deserializeRequestFromStream(baos.size(), dis);

        Assertions.assertArrayEquals(data, restored.getCachedBytes());
        Assertions.assertNull(restored.getData(), "V1-restored entry has no typed data");
        Assertions.assertFalse(restored.needsRehydration());
    }

    @Test
    void testNeedsRehydration() {
        // Both bytes and data — does not need rehydration (steady-state case)
        ClickHousePayload<String> full = new ClickHousePayload<>(new byte[]{1, 2}, "test");
        Assertions.assertFalse(full.needsRehydration());

        // Only data — needs rehydration (post-V2-restore case)
        ClickHousePayload<String> dehydrated = new ClickHousePayload<>("test");
        Assertions.assertTrue(dehydrated.needsRehydration());

        // Only bytes — does not need rehydration (V1-restore or string-mode case)
        ClickHousePayload<String> legacy = new ClickHousePayload<>(new byte[]{1, 2});
        Assertions.assertFalse(legacy.needsRehydration());
    }

    @Test
    void testV2RoundTripFallsBackToKryoForGenericTypes() throws Exception {
        // Force GenericTypeInfo (Kryo-backed) regardless of whether the class would
        // otherwise be picked up as a Flink POJO. Confirms V2 wire format works for
        // the fallback path, not just PojoSerializer.
        TypeSerializer<NonPojoRecord> kryoSerializer =
                new GenericTypeInfo<>(NonPojoRecord.class).createSerializer(new ExecutionConfig());
        ClickHouseAsyncSinkSerializer<NonPojoRecord> serializer =
                new ClickHouseAsyncSinkSerializer<>(kryoSerializer);

        NonPojoRecord original = new NonPojoRecord("abc", 42);
        ClickHousePayload<NonPojoRecord> payload = new ClickHousePayload<>(new byte[]{1, 2}, original);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        serializer.serializeRequestToStream(payload, dos);

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        ClickHousePayload<NonPojoRecord> restored =
                serializer.deserializeRequestFromStream(baos.size(), dis);

        Assertions.assertNull(restored.getCachedBytes());
        Assertions.assertEquals(original, restored.getData());
    }

    @Test
    void testJavaSerializeWrapperLosesTransientFields() throws Exception {
        // Documents the contract: ClickHousePayload<T>'s data and cachedBytes are transient.
        // The state serializer owns persistence; Java native serialization MUST NOT round-trip
        // them. This test guards against an accidental future change that drops `transient`.
        ClickHousePayload<String> original = new ClickHousePayload<>(new byte[]{1, 2, 3}, "hello");
        original.incrementAttempts(); // attemptCount is NOT transient
        original.incrementAttempts();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(original);
        }

        ClickHousePayload<String> restored;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            @SuppressWarnings("unchecked")
            ClickHousePayload<String> r = (ClickHousePayload<String>) ois.readObject();
            restored = r;
        }

        // attemptCount survives (non-transient): proves Java serialization actually ran
        Assertions.assertEquals(original.getAttemptCount(), restored.getAttemptCount());
        // data and cachedBytes are transient and must be wiped
        Assertions.assertNull(restored.getCachedBytes(),
                "cachedBytes is transient — must not survive Java native serialization");
        Assertions.assertNull(restored.getData(),
                "data is transient — must not survive Java native serialization");
    }

    @Test
    void testRehydrationLoopConvertsV2RestoredEntries() {
        // Stub converter: produces bytes "converted-" + input + verifies data is forwarded.
        ElementConverter<String, ClickHousePayload<String>> stubConverter = (s, ctx) ->
                new ClickHousePayload<>(("converted-" + s).getBytes(StandardCharsets.UTF_8), s);

        List<ClickHousePayload<String>> entries = new ArrayList<>();
        // (a) V2-restored: data only, needs rehydration
        entries.add(new ClickHousePayload<>("input1"));
        // (b) Steady-state: both fields populated, must not be touched
        entries.add(new ClickHousePayload<>(new byte[]{1, 2, 3}, "input2"));
        // (c) V1-restored: bytes only, must not be touched
        entries.add(new ClickHousePayload<>(new byte[]{4, 5, 6}));

        ClickHouseAsyncWriter.rehydrateIfNeeded(entries, stubConverter);

        // (a) bytes regenerated by the converter
        Assertions.assertArrayEquals(
                "converted-input1".getBytes(StandardCharsets.UTF_8),
                entries.get(0).getCachedBytes());
        Assertions.assertEquals("input1", entries.get(0).getData());

        // (b) untouched
        Assertions.assertArrayEquals(new byte[]{1, 2, 3}, entries.get(1).getCachedBytes());
        Assertions.assertEquals("input2", entries.get(1).getData());

        // (c) untouched
        Assertions.assertArrayEquals(new byte[]{4, 5, 6}, entries.get(2).getCachedBytes());
        Assertions.assertNull(entries.get(2).getData());
    }

    /** Non-POJO record used for the Kryo-fallback test (no no-arg ctor, no setters). */
    public static class NonPojoRecord implements Serializable {
        private static final long serialVersionUID = 1L;
        public final String id;
        public final int value;

        public NonPojoRecord(String id, int value) {
            this.id = id;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof NonPojoRecord)) return false;
            NonPojoRecord that = (NonPojoRecord) o;
            return value == that.value && Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, value);
        }
    }
}
