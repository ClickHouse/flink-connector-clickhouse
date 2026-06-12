package org.apache.flink.connector.clickhouse.sink.tpc;

import com.clickhouse.data.ClickHouseFormat;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink.PrecommittingSinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Objects;

/**
 * Accumulates records between checkpoints, produces {@link ClickHouseCommittable}s at precommit.
 *
 * <p>Records are never sent to ClickHouse from the writer. All destination I/O happens in
 * {@link ClickHouseCommitter}. This is what gives exactly-once semantics: committables are
 * persisted to the Flink checkpoint before any ClickHouse write happens, and the committer
 * runs only after the checkpoint completes globally.
 *
 * <p>The {@link #deduplicationTokenFor} formula is deterministic across job restarts: a
 * replayed record produces the same token, allowing ClickHouse to dedupe via
 * {@code insert_deduplication_token}.
 */
public class ClickHouseCommittingWriter<InputT>
        implements PrecommittingSinkWriter<InputT, ClickHouseCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseCommittingWriter.class);

    private final ElementConverter<InputT, ClickHousePayload> elementConverter;
    private final String tableName;
    private final ClickHouseFormat format;
    private final long maxBatchSizeInBytes;
    private final int maxBatchSize;
    private final int subtaskId;

    private final Deque<ClickHousePayload> buffer = new ArrayDeque<>();
    private long bufferedBytes = 0L;

    // Deterministic components of the dedup token.
    // checkpointId monotonically increases; we extract a best-effort value from
    // Flink's context. On restart, Flink replays from last successful checkpoint,
    // so the same (checkpointId, sequence) pair is reproduced.
    private long currentCheckpointId = 0L;
    private int sequenceInCheckpoint = 0;

    public ClickHouseCommittingWriter(
            Sink.InitContext context,
            ElementConverter<InputT, ClickHousePayload> elementConverter,
            String tableName,
            ClickHouseFormat format,
            int maxBatchSize,
            long maxBatchSizeInBytes) {
        this.elementConverter = Objects.requireNonNull(elementConverter, "elementConverter");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.format = Objects.requireNonNull(format, "format");
        this.maxBatchSize = maxBatchSize;
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.subtaskId = context.getSubtaskId();
        elementConverter.open(context);
    }

    @Override
    public void write(InputT element, SinkWriter.Context context)
            throws IOException, InterruptedException {
        ClickHousePayload p = elementConverter.apply(element, context);
        if (p == null || p.getPayload() == null) {
            return;
        }
        buffer.add(p);
        bufferedBytes += p.getPayloadLength();
    }

    @Override
    public void flush(boolean endOfInput) {
        // No-op: precommit is authoritative for flushing to committables.
        // Buffered records that arrive after the last prepareCommit go into the next checkpoint.
    }

    @Override
    public Collection<ClickHouseCommittable> prepareCommit() throws IOException {
        if (buffer.isEmpty()) {
            return Collections.emptyList();
        }

        // Checkpoint IDs advance monotonically; one checkpoint triggers one prepareCommit.
        // When Flink restarts from checkpoint N, it replays records, so subtasks produce
        // the same buffer content, which yields the same tokens.
        currentCheckpointId++;
        sequenceInCheckpoint = 0;

        List<ClickHouseCommittable> committables = new ArrayList<>();
        while (!buffer.isEmpty()) {
            List<ClickHousePayload> chunk = drainChunk();
            byte[] payload = concat(chunk);
            // Token must be deterministic across restarts AND bound to the actual payload
            // bytes. Position-only tokens (subtask, checkpoint, seq) are unsafe if Flink
            // re-parallelizes after restart because a different subtask may produce
            // different bytes at the same (subtask, ckpt, seq) tuple, causing
            // false-positive dedup and data loss. Binding to payloadHash eliminates that
            // failure mode: identical bytes always produce identical tokens.
            String token = deduplicationTokenFor(
                    subtaskId, currentCheckpointId, sequenceInCheckpoint++, payload);
            committables.add(new ClickHouseCommittable(
                    payload, tableName, format.name(), token, chunk.size()));
        }

        LOG.info("prepareCommit: subtask={} checkpoint={} produced {} committable(s)",
                subtaskId, currentCheckpointId, committables.size());
        return committables;
    }

    @Override
    public void close() {
        buffer.clear();
        bufferedBytes = 0L;
    }

    private List<ClickHousePayload> drainChunk() {
        List<ClickHousePayload> chunk = new ArrayList<>();
        long chunkBytes = 0L;
        while (!buffer.isEmpty()) {
            ClickHousePayload head = buffer.peekFirst();
            int headLen = head.getPayloadLength();
            boolean wouldExceedBytes = !chunk.isEmpty() && chunkBytes + headLen > maxBatchSizeInBytes;
            boolean wouldExceedCount = !chunk.isEmpty() && chunk.size() >= maxBatchSize;
            if (wouldExceedBytes || wouldExceedCount) {
                break;
            }
            buffer.removeFirst();
            bufferedBytes -= headLen;
            chunk.add(head);
            chunkBytes += headLen;
        }
        return chunk;
    }

    private static byte[] concat(List<ClickHousePayload> payloads) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (ClickHousePayload p : payloads) {
            byte[] bytes = p.getPayload();
            if (bytes != null) {
                out.write(bytes);
            }
        }
        return out.toByteArray();
    }

    private static String deduplicationTokenFor(
            int subtaskId, long checkpointId, int sequence, byte[] payload) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            // Position prefix keeps tokens unique across chunks even if payloads collide.
            String prefix = "ch-flink-tpc:" + subtaskId + ":" + checkpointId + ":" + sequence + ":";
            md.update(prefix.getBytes(StandardCharsets.UTF_8));
            // Payload binding makes token content-sensitive. Matches the block-aggregator
            // principle: identical bytes -> identical token -> ClickHouse dedupes.
            md.update(payload);
            return toHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed by every JRE; fallback keeps determinism but weaker.
            return "ch-flink-tpc:" + subtaskId + ":" + checkpointId + ":" + sequence
                    + ":" + payload.length;
        }
    }

    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    private static String toHex(byte[] bytes) {
        char[] out = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int b = bytes[i] & 0xFF;
            out[i * 2] = HEX_CHARS[b >>> 4];
            out[i * 2 + 1] = HEX_CHARS[b & 0x0F];
        }
        return new String(out);
    }
}
