package org.apache.flink.connector.clickhouse.sink;

import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
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
        ClickHousePayload deserialized = serializer.deserializeRequestFromStream(dos.size(), dis);
        Assertions.assertEquals(clickHousePayload.getPayloadLength(), deserialized.getPayloadLength());
        Assertions.assertArrayEquals(clickHousePayload.getPayload(), deserialized.getPayload());
    }

    @Test
    void testSerializeAndDeserializeNullPayload() throws Exception {
        ClickHousePayload clickHousePayload = new ClickHousePayload(null);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        ClickHouseAsyncSinkSerializer serializer = new ClickHouseAsyncSinkSerializer();
        serializer.serializeRequestToStream(clickHousePayload, dos);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        ClickHousePayload deserialized = serializer.deserializeRequestFromStream(dos.size(), dis);
        Assertions.assertEquals(clickHousePayload.getPayloadLength(), deserialized.getPayloadLength());
        Assertions.assertArrayEquals(clickHousePayload.getPayload(), deserialized.getPayload());
    }

    @Test
    void testSerializedSizeMatchesGetPayloadLength() throws Exception {
        byte[] data = "test-payload".getBytes();
        ClickHousePayload payload = new ClickHousePayload(data);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        ClickHouseAsyncSinkSerializer serializer = new ClickHouseAsyncSinkSerializer();
        serializer.serializeRequestToStream(payload, dos);

        Assertions.assertEquals(4 + data.length, baos.size());
    }

    @Test
    void testMultipleEntriesDeserializeCorrectly() throws Exception {
        byte[] data1 = "first".getBytes();
        byte[] data2 = "second-longer-payload".getBytes();
        byte[] data3 = "third".getBytes();

        ClickHouseAsyncSinkSerializer serializer = new ClickHouseAsyncSinkSerializer();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        ClickHousePayload p1 = new ClickHousePayload(data1);
        ClickHousePayload p2 = new ClickHousePayload(data2);
        ClickHousePayload p3 = new ClickHousePayload(data3);

        serializer.serializeRequestToStream(p1, dos);
        serializer.serializeRequestToStream(p2, dos);
        serializer.serializeRequestToStream(p3, dos);

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));

        ClickHousePayload r1 = serializer.deserializeRequestFromStream(p1.getPayloadLength(), dis);
        ClickHousePayload r2 = serializer.deserializeRequestFromStream(p2.getPayloadLength(), dis);
        ClickHousePayload r3 = serializer.deserializeRequestFromStream(p3.getPayloadLength(), dis);

        Assertions.assertArrayEquals(data1, r1.getPayload());
        Assertions.assertArrayEquals(data2, r2.getPayload());
        Assertions.assertArrayEquals(data3, r3.getPayload());
    }

    @Test
    void testV1StateIsDiscardedOnDeserialize() throws Exception {
        byte[] fakeV1State = new byte[]{0, 1, 2, 3, 4, 5};
        ClickHouseAsyncSinkSerializer serializer = new ClickHouseAsyncSinkSerializer();

        BufferedRequestState<ClickHousePayload> result = serializer.deserialize(1, fakeV1State);
        Assertions.assertTrue(result.getBufferedRequestEntries().isEmpty());
    }

    @Test
    void testDeserializeWithNegativeLength() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(-2); // invalid negative length (not -1 which means null)

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        ClickHouseAsyncSinkSerializer serializer = new ClickHouseAsyncSinkSerializer();

        Exception exception = Assertions.assertThrows(IOException.class, () -> {
            serializer.deserializeRequestFromStream(4, dis);
        });
        Assertions.assertTrue(exception.getMessage().contains("Invalid ClickHouse payload length"));
    }
}
