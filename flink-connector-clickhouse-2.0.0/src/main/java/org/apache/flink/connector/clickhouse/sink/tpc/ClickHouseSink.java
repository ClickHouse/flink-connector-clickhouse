package org.apache.flink.connector.clickhouse.sink.tpc;

import com.clickhouse.data.ClickHouseFormat;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Objects;

/**
 * Exactly-once ClickHouse sink built on Flink 2.0 {@link Sink} + {@link SupportsCommitter}
 * APIs.
 *
 * <p><b>Architecture:</b> idempotent-commit via ClickHouse dedup token.
 * <ul>
 *   <li>Writer ({@link ClickHouseCommittingWriter}) buffers records between checkpoints and
 *       produces {@link ClickHouseCommittable}s at precommit. No ClickHouse I/O happens in
 *       the writer.</li>
 *   <li>Committables carry a deterministic {@code insert_deduplication_token} derived from
 *       (subtaskId, checkpointId, sequenceInCheckpoint, payloadBytes). On restart, replayed
 *       committables produce identical tokens.</li>
 *   <li>Committer ({@link ClickHouseCommitter}) does the HTTP insert. A retry sends the
 *       same bytes with the same token; ClickHouse drops the second write.</li>
 * </ul>
 *
 * <p><b>Required ClickHouse table settings:</b>
 * <pre>
 *   ALTER TABLE &lt;replicated_target&gt; MODIFY SETTING
 *       replicated_deduplication_window = 10000,
 *       replicated_deduplication_window_seconds = 86400;
 * </pre>
 *
 * <p><b>Required Flink job config:</b>
 * <pre>
 *   env.enableCheckpointing(60_000);
 *   env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
 * </pre>
 *
 * <p><b>Source-determinism requirement:</b> Source operator must replay identical records
 * after failure (same bytes, same order, same subtask assignment). Kafka sources with fixed
 * parallelism and file sources satisfy this. Non-deterministic sources (e.g., window outputs
 * with late arrivals) do not and are unsafe with this sink.
 *
 * <p><b>Related prior art:</b>
 * <ul>
 *   <li>eBay Block Aggregator - same "deterministic block reconstruction + ClickHouse dedup"
 *       principle, implemented against a Kafka consumer in C++.</li>
 *   <li>ClickHouse ClickLoad script - uses a staging table plus {@code MOVE PARTITION} for
 *       atomic bulk loads. Stronger than dedup tokens when the dedup window cannot be sized
 *       to cover retry latency.</li>
 *   <li>{@code clickhouse-kafka-connect} - same deterministic-reconstruction pattern with
 *       state stored in a {@code KeeperMap} table.</li>
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
 * {@code writtenRowMismatches} counter analogous to eBay's ARV alarm.
 *
 * <p><b>Deliberate omissions vs. eBay Block Aggregator:</b>
 * <ul>
 *   <li>Cross-replica fencing - not applicable, Flink guarantees single-writer per committable.</li>
 *   <li>LocalLoaderLock / DistributedLoaderLock (ZooKeeper) - not needed, Flink's checkpoint
 *       alignment gives equivalent exclusivity.</li>
 *   <li>Reference offset - Flink source offsets + checkpoint id cover this.</li>
 *   <li>Quorum pre-check - revisit only if {@code insert_quorum} is set and quorum errors dominate.</li>
 *   <li>Schema evolution tracker - current scaffold requires job restart on schema change.</li>
 * </ul>
 */
public class ClickHouseSink<InputT>
        implements Sink<InputT>, SupportsCommitter<ClickHouseCommittable> {

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
    public SinkWriter<InputT> createWriter(WriterInitContext context) throws IOException {
        return new ClickHouseCommittingWriter<>(
                context,
                elementConverter,
                tableName,
                format,
                maxBatchSize,
                maxBatchSizeInBytes);
    }

    @Override
    public Committer<ClickHouseCommittable> createCommitter(CommitterInitContext context)
            throws IOException {
        ClickHouseCommitter committer = new ClickHouseCommitter(clientConfig);
        committer.registerMetrics(context);
        return committer;
    }

    @Override
    public SimpleVersionedSerializer<ClickHouseCommittable> getCommittableSerializer() {
        return new ClickHouseCommittableSerializer();
    }
}
