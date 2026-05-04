package org.apache.flink.connector.clickhouse.sink;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * State serializer for {@link ClickHousePayload}.
 *
 * <p>Wire format per entry (inside the framing supplied by
 * {@link AsyncSinkWriterStateSerializer}):
 *
 * <pre>
 * [version: int]
 *   if version == V1: [byteLen: int][bytes]      // restored to a payload with cachedBytes only
 *   if version == V2: [TypeSerializer.serialize(data, view)]
 * </pre>
 *
 * <p>V1 is the legacy bytes-only encoding produced by older versions of this connector.
 * It is preserved on the read path for backward compatibility with existing checkpoints.
 * V2 is the current encoding: only the typed user record is persisted, via Flink's
 * {@link TypeSerializer}; the RowBinary bytes are regenerated on first submit by the
 * configured {@code ElementConverter}.
 */
public class ClickHouseAsyncSinkSerializer<T>
        extends AsyncSinkWriterStateSerializer<ClickHousePayload<T>> {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseAsyncSinkSerializer.class);

    private static final int V1 = 1;
    private static final int V2 = 2;
    private static final int MAX_REASONABLE_PAYLOAD_BYTES = 256 * 1024 * 1024;

    private final TypeSerializer<T> inputSerializer;

    public ClickHouseAsyncSinkSerializer(TypeSerializer<T> inputSerializer) {
        this.inputSerializer = inputSerializer;
    }

    @Override
    protected void serializeRequestToStream(ClickHousePayload<T> entry, DataOutputStream out)
            throws IOException {
        T data = entry.getData();
        if (data != null) {
            out.writeInt(V2);
            inputSerializer.serialize(data, new DataOutputViewStreamWrapper(out));
        } else {
            byte[] bytes = entry.getCachedBytes();
            out.writeInt(V1);
            if (bytes != null) {
                out.writeInt(bytes.length);
                out.write(bytes);
            } else {
                out.writeInt(-1);
            }
        }
    }

    @Override
    protected ClickHousePayload<T> deserializeRequestFromStream(
            long requestSize, DataInputStream in) throws IOException {
        if (requestSize <= 0) {
            throw new IOException("Invalid request size: " + requestSize);
        }
        int version = in.readInt();
        switch (version) {
            case V1:
                return readV1(in);
            case V2:
                return readV2(in);
            default:
                throw new IOException("Unsupported serialization version: " + version);
        }
    }

    private ClickHousePayload<T> readV1(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len == -1) {
            return new ClickHousePayload<>((byte[]) null);
        }
        if (len < 0 || len > MAX_REASONABLE_PAYLOAD_BYTES) {
            throw new IOException("Implausible V1 payload length: " + len);
        }
        byte[] bytes = in.readNBytes(len);
        if (bytes.length != len) {
            throw new IOException("Truncated V1 payload: expected " + len + " bytes, got " + bytes.length);
        }
        return new ClickHousePayload<>(bytes);
    }

    private ClickHousePayload<T> readV2(DataInputStream in) throws IOException {
        T data = inputSerializer.deserialize(new DataInputViewStreamWrapper(in));
        if (data == null) {
            LOG.warn("Deserialized null data from V2 checkpoint state");
        }
        return new ClickHousePayload<>(data);
    }

    @Override
    public int getVersion() {
        return V2;
    }
}
