package org.apache.flink.connector.clickhouse.sink.tpc;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Serializes {@link ClickHouseCommittable} to/from Flink checkpoint state.
 *
 * <p>Checkpointed committables survive job failures and are replayed into
 * {@link ClickHouseCommitter#commit} on restart. Because the deduplication token is stored with
 * the committable, a replayed commit produces the same ClickHouse
 * {@code insert_deduplication_token} as the original attempt.
 */
public class ClickHouseCommittableSerializer
        implements SimpleVersionedSerializer<ClickHouseCommittable> {

    private static final int V1 = 1;

    @Override
    public int getVersion() {
        return V1;
    }

    @Override
    public byte[] serialize(ClickHouseCommittable c) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(c.getPayloadSize() + 256);
        try (DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(c.getTableName());
            out.writeUTF(c.getClickHouseFormat());
            out.writeUTF(c.getDeduplicationToken());
            out.writeInt(c.getRecordCount());
            byte[] payload = c.getPayload();
            out.writeInt(payload.length);
            out.write(payload);
        }
        return baos.toByteArray();
    }

    @Override
    public ClickHouseCommittable deserialize(int version, byte[] bytes) throws IOException {
        if (version != V1) {
            throw new IOException("Unsupported ClickHouseCommittable version: " + version);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            String tableName = in.readUTF();
            String format = in.readUTF();
            String token = in.readUTF();
            int recordCount = in.readInt();
            int len = in.readInt();
            byte[] payload = in.readNBytes(len);
            if (payload.length != len) {
                throw new IOException("Truncated payload: expected " + len + " got " + payload.length);
            }
            return new ClickHouseCommittable(payload, tableName, format, token, recordCount);
        }
    }
}
