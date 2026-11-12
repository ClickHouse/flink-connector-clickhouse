package org.apache.flink.connector.clickhouse.sink.tpc;

import com.clickhouse.data.ClickHouseFormat;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
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
 * Flink 2.0 writer that accumulates records between checkpoints and produces committables at
 * {@link #prepareCommit()}.
 *
 * <p>No ClickHouse I/O happens in the writer. Data is buffered in memory, chunked into
 * {@link ClickHouseCommittable}s at checkpoint barriers, and handed to {@link ClickHouseCommitter}
 * only after Flink globally acknowledges the checkpoint.
 *
 * <p>The deduplication token is bound to both position (subtaskId, checkpointId, seq) AND
 * payload bytes. Identical bytes always yield identical tokens, so a replayed record — even
 * landing on a different subtask after rescale — produces the same token and ClickHouse
 * dedupes the retry.
 */
public class ClickHouseCommittingWriter<InputT>
        implements CommittingSinkWriter<InputT, ClickHouseCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseCommittingWriter.class);

    private final ElementConverter<InputT, ClickHousePayload> elementConverter;
    private final String tableName;
    private final ClickHouseFormat format;
    private final long maxBatchSizeInBytes;
    private final int maxBatchSize;
    private final int subtaskId;

    private final Deque<ClickHousePayload> buffer = new ArrayDeque<>();
    private long bufferedBytes = 0L;

    // Monotonic across prepareCommit calls. Reset to the restored checkpoint id if Flink
    // recovered the writer; otherwise starts at 0 and increments per checkpoint.
    private long currentCheckpointId;
    private int sequenceInCheckpoint = 0;

    public ClickHouseCommittingWriter(
            WriterInitContext context,
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
        this.subtaskId = context.getTaskInfo().getIndexOfThisSubtask();
        this.currentCheckpointId = context.getRestoredCheckpointId().orElse(0L);
        elementConverter.open(context);
    }

    @Override
    public void write(InputT element, Context context)
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
        // No-op: prepareCommit is authoritative for draining to committables.
        // Buffered records after the last prepareCommit go into the next checkpoint.
        // On endOfInput, Flink still calls prepareCommit before close, so records drain cleanly.
    }

    @Override
    public Collection<ClickHouseCommittable> prepareCommit() throws IOException {
        if (buffer.isEmpty()) {
            return Collections.emptyList();
        }

        currentCheckpointId++;
        sequenceInCheckpoint = 0;

        List<ClickHouseCommittable> committables = new ArrayList<>();
        while (!buffer.isEmpty()) {
            List<ClickHousePayload> chunk = drainChunk();
            byte[] payload = concat(chunk);
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
            String prefix = "ch-flink-tpc:" + subtaskId + ":" + checkpointId + ":" + sequence + ":";
            md.update(prefix.getBytes(StandardCharsets.UTF_8));
            md.update(payload);
            return toHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
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
