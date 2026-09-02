package org.apache.flink.connector.clickhouse.sink;

import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.connector.clickhouse.data.TypeTags;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * State serializer for {@link ClickHousePayload}.
 *
 * <p>Per design spec §15. Parent {@link AsyncSinkWriterStateSerializer} owns the
 * per-blob framing; we override only the per-entry stream methods.
 *
 * <p>Three entry markers exist in the read path; only {@code ENTRY_MAP_LONG_STRINGS} is written:
 * <ul>
 *   <li>{@code ENTRY_BYTES_ONLY} — legacy V1 entries, read-only. Wrapped as
 *       {@code Map{RAW_KEY: bytes}} when {@code stringMode = true}; otherwise the
 *       restore fails with a drain-first error.</li>
 *   <li>{@code ENTRY_MAP} — V2 entries, read-only: tagged {@code Map<String,Object>}
 *       with writeUTF keys, which cap at 64 KB.</li>
 *   <li>{@code ENTRY_MAP_LONG_STRINGS} — V3 entries: keys and string values are int-length-prefixed
 *       UTF-8, so anything above writeUTF's 64 KB limit checkpoints safely.</li>
 * </ul>
 */
public class ClickHouseAsyncSinkSerializer
        extends AsyncSinkWriterStateSerializer<ClickHousePayload> {

    private static final int ENTRY_BYTES_ONLY = 1;
    // Map entry markers double as the TypeTags entry format version they were written under.
    private static final int ENTRY_MAP = TypeTags.V2;
    private static final int ENTRY_MAP_LONG_STRINGS = TypeTags.V3;
    private static final int MAX_REASONABLE_BYTES = 256 * 1024 * 1024;

    private final boolean stringMode;

    public ClickHouseAsyncSinkSerializer(boolean stringMode) {
        this.stringMode = stringMode;
    }

    @Override
    public int getVersion() { return TypeTags.V3; }

    @Override
    protected void serializeRequestToStream(ClickHousePayload entry, DataOutputStream out)
            throws IOException {
        out.writeInt(ENTRY_MAP_LONG_STRINGS);
        Map<String, Object> m = entry.getData();
        out.writeInt(m.size());
        for (Map.Entry<String, Object> e : m.entrySet()) {
            TypeTags.writeUtf8(e.getKey(), out);
            TypeTags.write(e.getValue(), out);
        }
    }

    @Override
    protected ClickHousePayload deserializeRequestFromStream(long requestSize, DataInputStream in)
            throws IOException {
        int marker = in.readInt();
        switch (marker) {
            case ENTRY_BYTES_ONLY: return readLegacyBytesOnly(in);
            case ENTRY_MAP:
            case ENTRY_MAP_LONG_STRINGS: return readMapEntry(in, marker);
            default: throw new IOException("Unknown entry marker: " + marker);
        }
    }

    private ClickHousePayload readLegacyBytesOnly(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] bytes;
        if (len == -1) {
            bytes = new byte[0];
        } else if (len < 0 || len > MAX_REASONABLE_BYTES) {
            throw new IOException("Implausible legacy bytes-only payload length: " + len);
        } else {
            bytes = new byte[len];
            in.readFully(bytes);
        }
        if (stringMode) {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put(ClickHousePayload.RAW_KEY, bytes);
            return ClickHousePayload.ofData(data);
        }
        throw new IOException(
            "Cannot restore legacy bytes-only checkpoint entry into a typed sink: "
            + "the wire format has changed from RowBinaryWithDefaults to "
            + "RowBinaryWithNamesAndTypes. Drain the previous sink before upgrading, "
            + "or configure this sink for STRING mode for one-shot pass-through.");
    }

    private ClickHousePayload readMapEntry(DataInputStream in, int entryVersion) throws IOException {
        int n = in.readInt();
        if (n < 0 || n > 1_000_000) {
            throw new IOException("Implausible map key count: " + n);
        }
        Map<String, Object> data = new LinkedHashMap<>(n);
        for (int i = 0; i < n; i++) {
            String key = entryVersion >= TypeTags.V3 ? TypeTags.readUtf8(in) : in.readUTF();
            data.put(key, TypeTags.read(in, entryVersion));
        }
        return ClickHousePayload.ofData(data);
    }
}
