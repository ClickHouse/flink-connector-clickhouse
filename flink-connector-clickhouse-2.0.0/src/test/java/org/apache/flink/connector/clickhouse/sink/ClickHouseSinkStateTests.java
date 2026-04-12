package org.apache.flink.connector.clickhouse.sink;

import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;

public class ClickHouseSinkStateTests {

    @Test
    void testSerializeAndDeserializePayload() throws Exception {
        byte[] data = {'H', 'e', 'l', 'l', 'o', 'W', 'o', 'r', 'l', 'd'};
        ClickHousePayload clickHousePayload = new ClickHousePayload(data);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        ClickHouseAsyncSinkSerializer serializer = new ClickHouseAsyncSinkSerializer();
        serializer.serializeRequestToStream(clickHousePayload, dos);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        ClickHousePayload clickHousePayload1 = serializer.deserializeRequestFromStream(dos.size(), dis);
        Assertions.assertEquals(clickHousePayload.getPayloadLength(), clickHousePayload1.getPayloadLength());
        Assertions.assertArrayEquals(clickHousePayload.getPayload(), clickHousePayload1.getPayload());
    }

    @Test
    void testSerializeAndDeserializeEmptyPayload() throws Exception {
        ClickHousePayload clickHousePayload = new ClickHousePayload(null);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        ClickHouseAsyncSinkSerializer serializer = new ClickHouseAsyncSinkSerializer();
        serializer.serializeRequestToStream(clickHousePayload, dos);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        ClickHousePayload clickHousePayload1 = serializer.deserializeRequestFromStream(dos.size(), dis);
        Assertions.assertEquals(clickHousePayload.getPayloadLength(), clickHousePayload1.getPayloadLength());
        Assertions.assertArrayEquals(clickHousePayload.getPayload(), clickHousePayload1.getPayload());
    }

    @Test
    void testDeserializePayloadWithUnsuportedVersion() throws IOException {
        byte[] data = {'H', 'e', 'l', 'l', 'o', 'W', 'o', 'r', 'l', 'd'};
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        int V3 = 3;
        dos.writeInt(V3);
        dos.writeInt(data.length);
        dos.write(data);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));

        ClickHouseAsyncSinkSerializer serializer = new ClickHouseAsyncSinkSerializer();
        Exception exception = Assertions.assertThrows(IOException.class, () -> {
            serializer.deserializeRequestFromStream(dos.size(), dis);
        });
        Assertions.assertEquals("Unsupported serialization version: 3", exception.getMessage());
    }

    @Test
    void testSerializeAndDeserializeWithOriginalInput() throws Exception {
        String originalInput = "Hello World";
        byte[] data = originalInput.getBytes();
        ClickHousePayload clickHousePayload = new ClickHousePayload(data, originalInput);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        ClickHouseAsyncSinkSerializer serializer = new ClickHouseAsyncSinkSerializer();
        serializer.serializeRequestToStream(clickHousePayload, dos);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));

        ClickHousePayload restored = serializer.deserializeRequestFromStream(baos.size(), dis);
        Assertions.assertNull(restored.getPayload(), "Payload should be null after V2 deserialization");
        Assertions.assertTrue(restored.needsRehydration(), "Restored payload should need rehydration");
        Assertions.assertEquals(originalInput, restored.getOriginalInput());
    }

    @Test
    void testV1BackwardCompatibility() throws Exception {
        // Manually write V1 format (as old code would)
        byte[] data = {'T', 'e', 's', 't'};
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(1); // V1
        dos.writeInt(data.length);
        dos.write(data);

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        ClickHouseAsyncSinkSerializer serializer = new ClickHouseAsyncSinkSerializer();
        ClickHousePayload restored = serializer.deserializeRequestFromStream(baos.size(), dis);

        Assertions.assertArrayEquals(data, restored.getPayload());
        Assertions.assertNull(restored.getOriginalInput(), "V1 restored payload should have no originalInput");
        Assertions.assertFalse(restored.needsRehydration());
    }

    @Test
    void testNeedsRehydration() {
        // Payload with both byte[] and originalInput — does not need rehydration
        ClickHousePayload full = new ClickHousePayload(new byte[]{1, 2}, "test");
        Assertions.assertFalse(full.needsRehydration());

        // Payload with only originalInput — needs rehydration
        ClickHousePayload dehydrated = new ClickHousePayload((java.io.Serializable) "test");
        Assertions.assertTrue(dehydrated.needsRehydration());

        // Payload with only byte[] — does not need rehydration
        ClickHousePayload legacy = new ClickHousePayload(new byte[]{1, 2});
        Assertions.assertFalse(legacy.needsRehydration());
    }
}
