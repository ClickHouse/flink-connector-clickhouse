package org.apache.flink.connector.clickhouse.sink;

import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

public class ClickHouseSinkStateTests {

    @Test
    void SerializerTest() throws Exception {
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
}
