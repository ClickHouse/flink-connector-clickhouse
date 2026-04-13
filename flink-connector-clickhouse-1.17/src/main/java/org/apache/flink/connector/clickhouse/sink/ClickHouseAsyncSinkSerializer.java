package org.apache.flink.connector.clickhouse.sink;

import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class ClickHouseAsyncSinkSerializer extends AsyncSinkWriterStateSerializer<ClickHousePayload> {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseAsyncSinkSerializer.class);
    private static final int V1 = 1;
    private static final int V2 = 2;

    @Override
    protected void serializeRequestToStream(ClickHousePayload clickHousePayload, DataOutputStream dataOutputStream) throws IOException {
        Serializable originalInput = clickHousePayload.getOriginalInput();
        if (originalInput != null) {
            dataOutputStream.writeInt(V2);
            ObjectOutputStream oos = new ObjectOutputStream(dataOutputStream);
            oos.writeObject(originalInput);
            oos.flush();
        } else {
            // Fallback to V1 for entries restored from old checkpoints without originalInput
            byte[] bytes = clickHousePayload.getPayload();
            dataOutputStream.writeInt(V1);
            if (bytes != null) {
                dataOutputStream.writeInt(bytes.length);
                dataOutputStream.write(bytes);
            } else {
                dataOutputStream.writeInt(-1);
            }
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

    private ClickHousePayload deserializeV2(DataInputStream dataInputStream) throws IOException {
        try {
            ObjectInputStream ois = new ObjectInputStream(dataInputStream);
            Serializable originalInput = (Serializable) ois.readObject();
            if (originalInput == null) {
                LOG.warn("Deserialized null originalInput from V2 checkpoint state");
            }
            return new ClickHousePayload(originalInput);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize original input from checkpoint", e);
        }
    }

    @Override
    protected ClickHousePayload deserializeRequestFromStream(long requestSize, DataInputStream dataInputStream) throws IOException {
        if (requestSize > 0) {
            int version = dataInputStream.readInt();
            if (version == V1) {
                return deserializeV1(dataInputStream);
            } else if (version == V2) {
                return deserializeV2(dataInputStream);
            } else {
                throw new IOException("Unsupported serialization version: " + version);
            }
        } else {
            throw new IOException("Invalid request size: " + requestSize);
        }
    }

    @Override
    public int getVersion() {
        return V2;
    }
}
