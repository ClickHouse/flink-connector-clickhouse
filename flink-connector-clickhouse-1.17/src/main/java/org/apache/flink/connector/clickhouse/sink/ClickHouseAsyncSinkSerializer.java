package org.apache.flink.connector.clickhouse.sink;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * State serializer for {@link ClickHousePayload}.
 *
 * <p>The blob version (returned by {@link #getVersion()}) marks the on-disk format
 * of the whole {@link BufferedRequestState} payload. The entry-level marker
 * embedded inside each entry distinguishes legacy bytes-only entries from
 * typed-data entries.
 *
 * <h3>Blob versions</h3>
 * <ul>
 *   <li><b>V1, V2</b> (legacy): no schema-evolution support. Entries are read using
 *       the current {@link TypeSerializer} directly — adding/removing POJO fields
 *       across upgrades will fail with EOFException or produce corrupt data.
 *       Read path delegates to the parent's
 *       {@link AsyncSinkWriterStateSerializer#deserialize(int, byte[])}.</li>
 *   <li><b>V3</b> (current): the blob carries a {@link TypeSerializerSnapshot}
 *       header captured at write time. On restore, the snapshot is consulted to
 *       resolve schema compatibility against the current {@link TypeSerializer},
 *       producing a migration-aware deserializer when fields have been
 *       added/removed/reordered.</li>
 * </ul>
 *
 * <h3>V3 wire format (the whole blob)</h3>
 * <pre>
 * [DATA_IDENTIFIER: long = -1]
 * [num_entries: int]
 * [TypeSerializerSnapshot bytes via TypeSerializerSnapshotSerializationUtil]
 * for each entry:
 *   [size_in_bytes: long]
 *   [entry_version: int]
 *     if entry_version == V1: [byteLen: int][bytes]
 *     if entry_version == V2: [TypeSerializer.serialize(data, view)]
 * </pre>
 */
public class ClickHouseAsyncSinkSerializer<T>
        extends AsyncSinkWriterStateSerializer<ClickHousePayload<T>> {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseAsyncSinkSerializer.class);

    private static final int V1 = 1;
    private static final int V2 = 2;
    private static final int V3 = 3;
    private static final long DATA_IDENTIFIER = -1;
    private static final int MAX_REASONABLE_PAYLOAD_BYTES = 256 * 1024 * 1024;

    private final TypeSerializer<T> inputSerializer;

    public ClickHouseAsyncSinkSerializer(TypeSerializer<T> inputSerializer) {
        this.inputSerializer = inputSerializer;
    }

    @Override
    public int getVersion() {
        return V3;
    }

    @Override
    public byte[] serialize(BufferedRequestState<ClickHousePayload<T>> obj) throws IOException {
        Collection<RequestEntryWrapper<ClickHousePayload<T>>> bufferState = obj.getBufferedRequestEntries();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeLong(DATA_IDENTIFIER);
            out.writeInt(bufferState.size());
            // V3: pin the schema by writing the current input serializer's snapshot.
            // On restore, this is what lets resolveSchemaCompatibility produce a
            // migration-aware deserializer when the POJO has changed shape.
            TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
                    new DataOutputViewStreamWrapper(out), inputSerializer.snapshotConfiguration());
            for (RequestEntryWrapper<ClickHousePayload<T>> wrapper : bufferState) {
                out.writeLong(wrapper.getSize());
                serializeRequestToStream(wrapper.getRequestEntry(), out);
            }
            return baos.toByteArray();
        }
    }

    @Override
    public BufferedRequestState<ClickHousePayload<T>> deserialize(int version, byte[] serialized)
            throws IOException {
        if (version < V3) {
            // Legacy blob: no snapshot header, no schema-evolution support. Falls back to
            // parent's per-entry deserialization via deserializeRequestFromStream, which
            // uses the current inputSerializer directly.
            return super.deserialize(version, serialized);
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            long identifier = in.readLong();
            if (identifier != DATA_IDENTIFIER) {
                throw new IOException("Invalid data identifier in V" + version + " blob: " + identifier);
            }
            int size = in.readInt();

            TypeSerializerSnapshot<T> oldSnapshot = TypeSerializerSnapshotSerializationUtil
                    .readSerializerSnapshot(new DataInputViewStreamWrapper(in), getClassLoader());
            TypeSerializer<T> readSerializer = resolveReadSerializer(oldSnapshot);

            List<RequestEntryWrapper<ClickHousePayload<T>>> entries = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                long requestSize = in.readLong();
                ClickHousePayload<T> payload = readEntry(in, readSerializer);
                entries.add(new RequestEntryWrapper<>(payload, requestSize));
            }
            return new BufferedRequestState<>(entries);
        }
    }

    /**
     * Compares the snapshot of the serializer that wrote the state with the current
     * {@link #inputSerializer} and picks the right serializer for reading old bytes
     * into instances of the current shape.
     */
    @SuppressWarnings("deprecation")
    private TypeSerializer<T> resolveReadSerializer(TypeSerializerSnapshot<T> oldSnapshot)
            throws IOException {
        // Flink 1.17 uses the older direction:
        // oldSnapshot.resolveSchemaCompatibility(newSerializer).
        // Flink 1.19+ flipped to newSnapshot.resolveSchemaCompatibility(oldSnapshot);
        // the 2.0.0 module uses the new form.
        TypeSerializerSchemaCompatibility<T> compat =
                oldSnapshot.resolveSchemaCompatibility(inputSerializer);
        if (compat.isCompatibleAsIs()) {
            return inputSerializer;
        }
        if (compat.isCompatibleAfterMigration()) {
            // For PojoSerializerSnapshot, restoreSerializer() returns a serializer
            // that reads old-shape bytes and produces new-shape instances with
            // defaults applied for fields added in the new schema.
            return oldSnapshot.restoreSerializer();
        }
        if (compat.isCompatibleWithReconfiguredSerializer()) {
            return compat.getReconfiguredSerializer();
        }
        throw new IOException(
                "Cannot deserialize ClickHouse sink writer state: schema incompatible. "
                        + "Old snapshot: " + oldSnapshot.getClass().getName()
                        + "; new serializer: " + inputSerializer);
    }

    private ClickHousePayload<T> readEntry(DataInputStream in, TypeSerializer<T> readSerializer)
            throws IOException {
        int entryVersion = in.readInt();
        switch (entryVersion) {
            case V1:
                return readV1(in);
            case V2:
                T data = readSerializer.deserialize(new DataInputViewStreamWrapper(in));
                if (data == null) {
                    LOG.warn("Deserialized null data from V3 blob (V2 entry)");
                }
                return new ClickHousePayload<>(data);
            default:
                throw new IOException("Unsupported entry version: " + entryVersion);
        }
    }

    private ClassLoader getClassLoader() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        return cl != null ? cl : getClass().getClassLoader();
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

    /**
     * Used by {@link AsyncSinkWriterStateSerializer#deserialize(int, byte[])} when
     * reading legacy V1/V2 blobs (no snapshot header). No schema-evolution support
     * on this path — the current {@link #inputSerializer} reads bytes directly.
     */
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
}
