package org.apache.flink.connector.clickhouse.sink;

import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ClickHouseAsyncSinkSerializer extends AsyncSinkWriterStateSerializer<ClickHousePayload> {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseAsyncSinkSerializer.class);
    private static final int V1 = 1;
    @Override
    protected void serializeRequestToStream(ClickHousePayload clickHousePayload, DataOutputStream dataOutputStream) throws IOException {
        byte[] bytes = clickHousePayload.getPayload();
        if (bytes != null) {
            dataOutputStream.writeInt(V1);
            dataOutputStream.writeInt(bytes.length);
            dataOutputStream.write(bytes);
        } else {
            dataOutputStream.writeInt(-1);
        }

    }

    private ClickHousePayload deserializeV1(DataInputStream dataInputStream) throws IOException {
        int len = dataInputStream.readInt();
        if (len == -1) {
            return new ClickHousePayload(null);
        }
        byte[] payload = dataInputStream.readNBytes(len);
        return new ClickHousePayload(payload);
    }

    @Override
    protected ClickHousePayload deserializeRequestFromStream(long requestSize, DataInputStream dataInputStream) throws IOException {
        if  (requestSize > 0) {
            int version = dataInputStream.readInt();
            if (version == V1) {
                return deserializeV1(dataInputStream);
            } else {
                throw new IOException("Unsupported version " + version);
            }
        } else {
            throw new IOException("Request size: " + requestSize);
        }
    }

    @Override
    public int getVersion() {
        return V1;
    }
}
