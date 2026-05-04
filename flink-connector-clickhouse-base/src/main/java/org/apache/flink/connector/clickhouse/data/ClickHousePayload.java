package org.apache.flink.connector.clickhouse.data;

import java.io.Serializable;

/**
 * Buffered request entry held by the ClickHouse async sink.
 *
 * <p>Holds two transient projections of the same logical record:
 * <ul>
 *   <li>{@code data} — the user-typed source of truth, used for state checkpointing
 *       via Flink's {@link org.apache.flink.api.common.typeutils.TypeSerializer}.
 *   <li>{@code cachedBytes} — the converted RowBinary (or other format) bytes that are
 *       actually sent to ClickHouse on submit. Derived from {@code data} by the
 *       configured {@link org.apache.flink.connector.base.sink.writer.ElementConverter}.
 * </ul>
 *
 * <p>Both fields are {@code transient}; checkpoint serialization is owned by
 * {@code ClickHouseAsyncSinkSerializer}, which writes only {@code data} (V2) or only
 * {@code cachedBytes} (V1, for backward compatibility on restore from older checkpoints).
 *
 * <p>The wrapper itself implements {@link Serializable} only because Flink's
 * {@code BufferedRequestState&lt;RequestEntryT extends Serializable&gt;} requires it as a
 * type-system upper bound — Java native serialization is never used for this class.
 */
public class ClickHousePayload<T> implements Serializable {
    private static final long serialVersionUID = 3L;

    private int attemptCount = 1;
    private transient byte[] cachedBytes;
    private transient T data;

    /** Used by V1 deserialization or string-mode entries: bytes only, no typed data. */
    public ClickHousePayload(byte[] cachedBytes) {
        this.cachedBytes = cachedBytes;
    }

    /** Used by the element converter in steady state: both projections present. */
    public ClickHousePayload(byte[] cachedBytes, T data) {
        this.cachedBytes = cachedBytes;
        this.data = data;
    }

    /** Used by V2 deserialization on restore: typed data only, bytes regenerated lazily. */
    public ClickHousePayload(T data) {
        this.cachedBytes = null;
        this.data = data;
    }

    public byte[] getCachedBytes() { return cachedBytes; }
    public void setCachedBytes(byte[] cachedBytes) { this.cachedBytes = cachedBytes; }

    public int getCachedBytesLength() {
        return cachedBytes == null ? -1 : cachedBytes.length;
    }

    public T getData() { return data; }

    /** True when we have the source-of-truth data but no bytes yet (post-V2-restore state). */
    public boolean needsRehydration() {
        return cachedBytes == null && data != null;
    }

    public int getAttemptCount() { return attemptCount; }
    public void incrementAttempts() { attemptCount++; }
}
