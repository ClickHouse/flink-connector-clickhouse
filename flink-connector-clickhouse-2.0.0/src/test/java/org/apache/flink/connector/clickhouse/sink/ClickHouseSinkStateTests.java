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
        DataOutputStream dataOutputStream = new DataOutputStream(baos);
        int V2 = 2;
        dataOutputStream.writeInt(V2);
        dataOutputStream.writeInt(data.length);
        dataOutputStream.write(data);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));

        ClickHouseAsyncSinkSerializer serializer = new ClickHouseAsyncSinkSerializer();
        Exception exception = Assertions.assertThrows(IOException.class, () -> {
            serializer.deserializeRequestFromStream(dataOutputStream.size(), dis);
        });
        Assertions.assertEquals("Unsupported serialization version: 2", exception.getMessage());
    }
}
