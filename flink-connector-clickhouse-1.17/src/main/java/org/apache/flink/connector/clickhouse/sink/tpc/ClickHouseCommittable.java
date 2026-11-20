package org.apache.flink.connector.clickhouse.sink.tpc;

import java.io.Serializable;
import java.util.Objects;

/**
 * Unit of work committed atomically to ClickHouse.
 *
 * <p>Produced by {@link ClickHouseCommittingWriter#prepareCommit()} and consumed by
 * {@link ClickHouseCommitter#commit}. The {@link #deduplicationToken} is deterministic across
 * restarts (derived from subtaskId + checkpointId + sequenceInCheckpoint), which guarantees
 * that a replayed committable produces the exact same token. ClickHouse's
 * {@code insert_deduplication_token} setting then collapses duplicate inserts.
 */
public class ClickHouseCommittable implements Serializable {
    private static final long serialVersionUID = 1L;

    private final byte[] payload;
    private final String tableName;
    private final String clickHouseFormat;
    private final String deduplicationToken;
    private final int recordCount;

    public ClickHouseCommittable(
            byte[] payload,
            String tableName,
            String clickHouseFormat,
            String deduplicationToken,
            int recordCount) {
        this.payload = Objects.requireNonNull(payload, "payload");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.clickHouseFormat = Objects.requireNonNull(clickHouseFormat, "clickHouseFormat");
        this.deduplicationToken = Objects.requireNonNull(deduplicationToken, "deduplicationToken");
        this.recordCount = recordCount;
    }

    public byte[] getPayload() {
        return payload;
    }

    public String getTableName() {
        return tableName;
    }

    public String getClickHouseFormat() {
        return clickHouseFormat;
    }

    public String getDeduplicationToken() {
        return deduplicationToken;
    }

    public int getRecordCount() {
        return recordCount;
    }

    public int getPayloadSize() {
        return payload.length;
    }

    @Override
    public String toString() {
        return "ClickHouseCommittable{table=" + tableName
                + ", format=" + clickHouseFormat
                + ", token=" + deduplicationToken
                + ", records=" + recordCount
                + ", bytes=" + payload.length + '}';
    }
}
