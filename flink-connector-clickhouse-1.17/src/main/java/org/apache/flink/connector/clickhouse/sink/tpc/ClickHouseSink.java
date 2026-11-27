package org.apache.flink.connector.clickhouse.sink.tpc;

import com.clickhouse.data.ClickHouseFormat;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Objects;

/**
 * Exactly-once ClickHouse sink built on Flink's {@link TwoPhaseCommittingSink}.
 *
 * <p>Architecture: "idempotent-commit via ClickHouse dedup token".
 * <ul>
 *   <li>Writer ({@link ClickHouseCommittingWriter}) buffers records between checkpoints and
 *       produces {@link ClickHouseCommittable}s at precommit. No ClickHouse I/O happens in
 *       the writer.</li>
 *   <li>Committables carry a deterministic {@code insert_deduplication_token} derived from
 *       (subtaskId, checkpointId, sequenceInCheckpoint). On restart, replayed committables
 *       produce identical tokens.</li>
 *   <li>Committer ({@link ClickHouseCommitter}) does the HTTP insert. A retry sends the
 *       same bytes with the same token; ClickHouse drops the second write.</li>
 * </ul>
 *
 * <p>Required ClickHouse table settings for safe operation:
 * <pre>
 *   ALTER TABLE &lt;replicated_target&gt; MODIFY SETTING
 *       replicated_deduplication_window = 10000,
 *       replicated_deduplication_window_seconds = 86400;
 * </pre>
 *
 * <p>Required Flink job config:
 * <pre>
 *   env.enableCheckpointing(60_000);
 *   env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
 * </pre>
 *
 * <p><b>Source-determinism requirement:</b> This sink relies on the source operator replaying
 * identical records after a failure (same bytes, same order, same subtask assignment).
 * Kafka sources with fixed parallelism and file sources satisfy this. Non-deterministic
 * sources (e.g., window outputs with late arrivals) do not and are unsafe with this sink.
 *
 * <p><b>Related prior art:</b>
 * <ul>
 *   <li>eBay Block Aggregator - same "deterministic block reconstruction + ClickHouse dedup"
 *       principle, implemented against a Kafka consumer in C++. Key differences: eBay stores
 *       per-table (start, end) offsets in Kafka's {@code __consumer_offset} metadata, while
 *       this sink stores the full serialized payload in Flink checkpoint state.</li>
 *   <li>ClickHouse's official {@code ClickLoad} script - uses a staging table plus
 *       {@code ALTER TABLE ... MOVE PARTITION} for atomic exactly-once bulk loads.
 *       That pattern gives stronger guarantees than dedup tokens when the dedup window
 *       cannot be sized to cover retry latency.</li>
 *   <li>{@code clickhouse-kafka-connect} - same deterministic-reconstruction pattern
 *       with state stored in a {@code KeeperMap} table.</li>
 * </ul>
 *
 * <p><b>Multi-replica / multi-DC safety:</b> ClickHouse's block-hash dedup plus this sink's
 * content-bound {@code insert_deduplication_token} let multiple producers target the same
 * replicated table without coordination. If a Flink subtask and an external recovery process
 * both attempt the same commit, identical bytes + identical tokens deduplicate at the shard's
 * {@code ReplicatedMergeTree} level.
 *
 * <p><b>Observability:</b> {@link ClickHouseCommitter#registerMetrics} exposes counters for
 * succeeded/retried/failed commits, records/bytes committed, commit latency, and a
 * {@code writtenRowMismatches} counter that is incremented when the server's reported
 * written-row count exceeds the committable's expected count. Watch the mismatch counter in
 * production - non-zero is the signal for a dedup anomaly analogous to eBay's ARV (Aggregator
 * Runtime Verifier) alarms. A future companion verifier could subscribe to Flink's committed
 * committable log and validate monotonic progress across checkpoints.
 *
 * <p><b>Deliberate omissions vs. eBay Block Aggregator:</b>
 * <ul>
 *   <li><b>Cross-replica fencing</b> (eBay: compare currentMetadata vs previousMetadata on
 *       Kafka before commit). Not applicable - Flink guarantees single-writer ownership per
 *       committable within a checkpoint, so there is no sibling replica to fence against.</li>
 *   <li><b>LocalLoaderLock and DistributedLoaderLock (ZooKeeper)</b> (eBay: two-level
 *       locking to serialize inserts per table). Not needed - Flink's checkpoint alignment
 *       plus 2PC commit ordering give equivalent exclusivity without a distributed lock.</li>
 *   <li><b>Reference offset</b> (eBay: monotonic tag for gap detection across tables).
 *       Flink source offsets cover this; the checkpoint barrier sets an equivalent epoch.</li>
 *   <li><b>Quorum pre-check</b> (eBay: {@code SELECT FROM system.zookeeper WHERE
 *       path='/clickhouse/tables/&#123;shard&#125;/&#123;table&#125;/quorum'} before retry
 *       when {@code insert_quorum} is set). Trade-off: extra ZK lookup per retry. Revisit
 *       if {@code insert_quorum} is enabled and quorum errors dominate.</li>
 *   <li><b>ZooKeeper heartbeat</b> before retry (eBay). Flink's own health checks make
 *       this redundant.</li>
 *   <li><b>Schema evolution tracker</b> (eBay: {@code TableSchemaUpdateTracker} refetches
 *       schema on mismatch and rebuilds block). Current scaffold requires a job restart on
 *       schema change. Future work.</li>
 * </ul>
 *
 * <p>This sink uses the token path because Flink's 2PC protocol persists committables in
 * checkpoint state, so the dedup window constraint is rarely binding in practice. A future
 * staging-table mode can be added as an alternative for strict-zero-duplicate deployments.
 */
public class ClickHouseSink<InputT>
        implements TwoPhaseCommittingSink<InputT, ClickHouseCommittable> {

    private static final long serialVersionUID = 1L;

    private final ElementConverter<InputT, ClickHousePayload> elementConverter;
    private final ClickHouseClientConfig clientConfig;
    private final String tableName;
    private final ClickHouseFormat format;
    private final int maxBatchSize;
    private final long maxBatchSizeInBytes;

    public ClickHouseSink(
            ElementConverter<InputT, ClickHousePayload> elementConverter,
            ClickHouseClientConfig clientConfig,
            ClickHouseFormat format,
            int maxBatchSize,
            long maxBatchSizeInBytes) {
        this.elementConverter = Objects.requireNonNull(elementConverter, "elementConverter");
        this.clientConfig = Objects.requireNonNull(clientConfig, "clientConfig");
        this.format = Objects.requireNonNull(format, "format");
        this.tableName = Objects.requireNonNull(clientConfig.getTableName(), "clientConfig.tableName");
        this.maxBatchSize = maxBatchSize;
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
    }

    @Override
    public PrecommittingSinkWriter<InputT, ClickHouseCommittable> createWriter(InitContext context)
            throws IOException {
        return new ClickHouseCommittingWriter<>(
                context,
                elementConverter,
                tableName,
                format,
                maxBatchSize,
                maxBatchSizeInBytes);
    }

    @Override
    public Committer<ClickHouseCommittable> createCommitter() throws IOException {
        return new ClickHouseCommitter(clientConfig);
    }

    @Override
    public SimpleVersionedSerializer<ClickHouseCommittable> getCommittableSerializer() {
        return new ClickHouseCommittableSerializer();
    }
}
