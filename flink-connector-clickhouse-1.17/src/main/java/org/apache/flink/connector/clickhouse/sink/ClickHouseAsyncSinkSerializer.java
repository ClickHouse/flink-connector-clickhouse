package org.apache.flink.connector.clickhouse.sink;

import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;

public class ClickHouseAsyncSinkSerializer extends AsyncSinkWriterStateSerializer<ClickHousePayload> {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseAsyncSinkSerializer.class);

    private static final int V1 = 1;
    private static final int V2 = 2;
    private static final int NULL_PAYLOAD_LENGTH = -1;

    @Override
    public int getVersion() {
        return V2;
    }

    @Override
    protected void serializeRequestToStream(ClickHousePayload payload, DataOutputStream out) throws IOException {
        byte[] bytes = payload.getPayload();
        if (bytes == null) {
            out.writeInt(NULL_PAYLOAD_LENGTH);
            return;
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Override
    protected ClickHousePayload deserializeRequestFromStream(long requestSize, DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length == NULL_PAYLOAD_LENGTH) {
            return new ClickHousePayload(null);
        }
        if (length < 0) {
            throw new IOException("Invalid ClickHouse payload length: " + length);
        }
        byte[] bytes = in.readNBytes(length);
        if (bytes.length != length) {
            throw new IOException(
                    "Truncated ClickHouse payload: expected " + length + " bytes, got " + bytes.length);
        }
        return new ClickHousePayload(bytes);
    }

    /**
     * Discards checkpoints written by the broken V1 serializer to prevent
     * restore loops. Everything else delegates to the base implementation.
     */
    @Override
    public BufferedRequestState<ClickHousePayload> deserialize(int version, byte[] serialized) throws IOException {
        if (version == V1) {
            LOG.warn("Discarding ClickHouse sink state from version {} ({} bytes). "
                            + "Records will be re-emitted from the source.",
                    V1, serialized.length);
            return new BufferedRequestState<>(Collections.emptyList());
        }
        return super.deserialize(version, serialized);
    }
}
