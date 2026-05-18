package org.apache.flink.connector.clickhouse.data;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Buffered request entry held by the ClickHouse async sink.
 *
 * <p>The Map is the canonical source of truth for checkpointed state; bytes are
 * derived (eagerly at apply-time, lazily after restore via the configured
 * {@link org.apache.flink.connector.clickhouse.convertor.DataMapper}'s bindings).
 *
 * <p>See design spec §4 in docs/superpowers/specs/2026-05-16-map-based-payload-design.md.
 */
public class ClickHousePayload implements Serializable {
    private static final long serialVersionUID = 4L;

    /** Reserved Map key for STRING-mode raw payloads. Value type: byte[]. */
    public static final String RAW_KEY = "__clickhouse_raw__";

    private transient byte[] cachedBytes;
    private final Map<String, Object> data;
    private int attemptCount = 1;

    /** Fresh write: empty mutable map, bytes filled in by caller. */
    public ClickHousePayload() {
        this.data = new LinkedHashMap<>();
    }

    /** V2 restore: deserialized map; bytes regenerated lazily on flush. */
    public ClickHousePayload(Map<String, Object> data) {
        this.data = data;
    }

    /** STRING-mode constructor: wraps raw bytes under {@link #RAW_KEY}. */
    public static ClickHousePayload ofRaw(byte[] bytes) {
        ClickHousePayload p = new ClickHousePayload();
        p.data.put(RAW_KEY, bytes);
        p.cachedBytes = bytes;
        return p;
    }

    public Map<String, Object> getData() { return data; }
    public byte[] getCachedBytes() { return cachedBytes; }
    public void setCachedBytes(byte[] b) { this.cachedBytes = b; }

    public int getCachedBytesLength() {
        return cachedBytes == null ? -1 : cachedBytes.length;
    }

    public boolean isRaw() {
        return data != null && data.containsKey(RAW_KEY);
    }

    public boolean needsRehydration() {
        return cachedBytes == null;
    }

    public int getAttemptCount() { return attemptCount; }
    public void incrementAttempts() { attemptCount++; }
}
