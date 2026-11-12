package org.apache.flink.connector.clickhouse.sink.tpc;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.data.ClickHouseFormat;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Commits {@link ClickHouseCommittable}s to ClickHouse idempotently.
 *
 * <p>The commit is always a blocking, synchronous HTTP insert with
 * {@code insert_deduplication_token} set. Retries use the same token, so ClickHouse drops
 * the second write if the first actually landed. This is the crux of the exactly-once
 * guarantee: Flink's 2PC protocol guarantees the committable exists in checkpoint state
 * until commit succeeds, and ClickHouse's dedup token guarantees the commit itself is
 * idempotent.
 *
 * <p>Retryable vs fatal failure classification is deliberately broad: any IOException or
 * client-side transient is retried. Non-transient errors (schema mismatch, auth failure)
 * propagate via {@code signalFailedWithKnownReason}, which fails the job and forces a
 * restart from the last good checkpoint.
 */
public class ClickHouseCommitter implements Committer<ClickHouseCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseCommitter.class);
    private static final long DEFAULT_COMMIT_TIMEOUT_MS = 120_000L;

    private final ClickHouseClientConfig clientConfig;
    private final long commitTimeoutMs;
    private transient Client chClient;
    // Track consecutive connection-level failures so we can proactively recycle the client.
    // Rationale: eBay Block Aggregator recreates the CH connection on every attempt to avoid
    // stale-socket pitfalls; doing that on every commit is too expensive for Flink's cadence,
    // so we reset only after consecutive failures. See BlockSupportedBufferFlushTask::doBlockInsertion.
    private transient int consecutiveConnectionFailures;
    private static final int CONNECTION_RESET_AFTER_FAILURES = 2;

    // Observability — inspired by eBay Block Aggregator's 100+ metrics.
    // Operators need these to detect silent degradation (dedup anomalies,
    // commit latency drift, sustained transient error rates).
    private transient Counter commitsSucceeded;
    private transient Counter commitsRetried;
    private transient Counter commitsFailedPermanent;
    private transient Counter recordsCommitted;
    private transient Counter bytesCommitted;
    private transient Counter writtenRowMismatches;
    private transient Histogram commitLatencyMs;

    public ClickHouseCommitter(ClickHouseClientConfig clientConfig) {
        this(clientConfig, DEFAULT_COMMIT_TIMEOUT_MS);
    }

    public ClickHouseCommitter(ClickHouseClientConfig clientConfig, long commitTimeoutMs) {
        this.clientConfig = clientConfig;
        this.commitTimeoutMs = commitTimeoutMs;
    }

    /**
     * Wire metrics. Called by the sink framework before first commit. Safe to skip if the
     * caller isn't using metric-aware bootstrapping — no-op metrics are set up lazily.
     */
    public void registerMetrics(MetricGroup group) {
        MetricGroup ch = group.addGroup("clickhouseCommitter");
        commitsSucceeded = ch.counter("commitsSucceeded");
        commitsRetried = ch.counter("commitsRetried");
        commitsFailedPermanent = ch.counter("commitsFailedPermanent");
        recordsCommitted = ch.counter("recordsCommitted");
        bytesCommitted = ch.counter("bytesCommitted");
        writtenRowMismatches = ch.counter("writtenRowMismatches");
        commitLatencyMs = ch.histogram("commitLatencyMs", new DescriptiveStatisticsHistogram(1000));
    }

    private Client client() {
        if (chClient == null) {
            chClient = clientConfig.createClient();
        }
        return chClient;
    }

    @Override
    public void commit(Collection<CommitRequest<ClickHouseCommittable>> requests)
            throws IOException, InterruptedException {
        for (CommitRequest<ClickHouseCommittable> request : requests) {
            commitOne(request);
        }
    }

    private void commitOne(CommitRequest<ClickHouseCommittable> request) {
        ClickHouseCommittable c = request.getCommittable();
        InsertSettings settings = buildInsertSettings(c);
        long start = System.currentTimeMillis();

        try {
            CompletableFuture<InsertResponse> future = client().insert(
                    c.getTableName(),
                    out -> {
                        out.write(c.getPayload());
                        out.close();
                    },
                    ClickHouseFormat.valueOf(c.getClickHouseFormat()),
                    settings);

            InsertResponse response = future.get(commitTimeoutMs, TimeUnit.MILLISECONDS);
            long latency = System.currentTimeMillis() - start;
            consecutiveConnectionFailures = 0;  // reset on any successful round-trip

            // Anomaly detection (inspired by eBay's Aggregator Runtime Verifier).
            // ClickHouse returns the number of rows actually written after server-side
            // deduplication by insert_deduplication_token. Three legitimate outcomes:
            //   writtenRows == recordCount : first successful attempt.
            //   writtenRows == 0           : token matched prior commit, dedup fired (retry landed).
            //   writtenRows > 0 && < count : partial success on Distributed table (some shards
            //                                deduped, others inserted). Still correct.
            // Anything else (e.g., writtenRows > recordCount) signals an upstream anomaly.
            long writtenRows = response.getWrittenRows();
            if (writtenRows > c.getRecordCount()) {
                LOG.error("Anomaly: writtenRows={} exceeds recordCount={} for token={}. " +
                        "Investigate Distributed sharding or connector layering.",
                        writtenRows, c.getRecordCount(), c.getDeduplicationToken());
                incr(writtenRowMismatches);
            }

            incr(commitsSucceeded);
            if (recordsCommitted != null) recordsCommitted.inc(c.getRecordCount());
            if (bytesCommitted != null) bytesCommitted.inc(c.getPayloadSize());
            if (commitLatencyMs != null) commitLatencyMs.update(latency);

            LOG.info("Commit ok: token={} records={} written={} bytes={} latencyMs={} queryId={}",
                    c.getDeduplicationToken(),
                    c.getRecordCount(),
                    writtenRows,
                    c.getPayloadSize(),
                    latency,
                    response.getQueryId());
        } catch (Exception e) {
            if (commitLatencyMs != null) commitLatencyMs.update(System.currentTimeMillis() - start);
            handleFailure(request, c, e);
        }
    }

    private static void incr(Counter c) {
        if (c != null) c.inc();
    }

    private void handleFailure(
            CommitRequest<ClickHouseCommittable> request,
            ClickHouseCommittable c,
            Exception e) {
        if (isRetriable(e)) {
            LOG.warn("Commit transient failure (attempt {}): token={} records={} - will retry",
                    request.getNumberOfRetries() + 1,
                    c.getDeduplicationToken(),
                    c.getRecordCount(),
                    e);
            incr(commitsRetried);

            // If the failure looks connection-level, recycle the client so the next retry
            // builds a fresh HTTP pool. Matches eBay's fresh-connection-per-attempt practice
            // (see BlockSupportedBufferFlushTask::doBlockInsertion) scaled to Flink's cadence.
            if (isConnectionLevel(e)) {
                consecutiveConnectionFailures++;
                if (consecutiveConnectionFailures >= CONNECTION_RESET_AFTER_FAILURES) {
                    LOG.warn("Recycling ClickHouse client after {} consecutive connection-level failures",
                            consecutiveConnectionFailures);
                    recycleClient();
                    consecutiveConnectionFailures = 0;
                }
            }

            request.retryLater();
        } else {
            LOG.error("Commit permanent failure: token={} records={} - failing job",
                    c.getDeduplicationToken(),
                    c.getRecordCount(),
                    e);
            incr(commitsFailedPermanent);
            request.signalFailedWithKnownReason(e);
        }
    }

    private static boolean isConnectionLevel(Throwable e) {
        Throwable cause = unwrap(e);
        if (cause instanceof TimeoutException) return true;
        if (cause instanceof java.net.SocketTimeoutException) return true;
        if (cause instanceof java.net.ConnectException) return true;
        if (cause instanceof java.net.SocketException) return true;
        if (cause instanceof java.io.IOException) return true;
        String msg = cause == null ? "" : String.valueOf(cause.getMessage()).toLowerCase();
        return msg.contains("connection reset")
                || msg.contains("broken pipe")
                || msg.contains("connection closed")
                || msg.contains("read timed out");
    }

    private void recycleClient() {
        if (chClient != null) {
            try {
                chClient.close();
            } catch (Exception closeEx) {
                LOG.warn("Ignoring error while closing stale ClickHouse client", closeEx);
            }
            chClient = null;
        }
    }

    /**
     * Transient errors that should be retried with the same dedup token. Anything else is
     * treated as permanent and fails the commit (and therefore the job).
     */
    private static boolean isRetriable(Throwable e) {
        Throwable cause = unwrap(e);
        if (cause instanceof TimeoutException) return true;
        if (cause instanceof java.net.SocketTimeoutException) return true;
        if (cause instanceof java.net.ConnectException) return true;
        if (cause instanceof java.io.IOException) return true;

        // ClickHouse server error codes known to be transient. Prefer numeric codes over
        // message strings so classification survives localization and minor message changes.
        // Extracted from the ClickHouse exception text like "Code: 252. DB::Exception: ...".
        int code = extractClickHouseErrorCode(cause);
        if (code > 0) {
            switch (code) {
                case 76:   // CANNOT_OPEN_FILE
                case 209:  // SOCKET_TIMEOUT
                case 210:  // NETWORK_ERROR
                case 241:  // MEMORY_LIMIT_EXCEEDED
                case 242:  // TABLE_IS_READ_ONLY
                case 246:  // CORRUPTED_DATA (often transient — IO flap)
                case 252:  // TOO_MANY_PARTS
                case 319:  // UNKNOWN_STATUS_OF_INSERT
                case 999:  // KEEPER_EXCEPTION
                    return true;
                case 27:   // CANNOT_PARSE_INPUT_ASSERTION_FAILED
                case 36:   // BAD_ARGUMENTS
                case 47:   // UNKNOWN_IDENTIFIER
                case 60:   // UNKNOWN_TABLE
                case 62:   // SYNTAX_ERROR
                case 81:   // UNKNOWN_DATABASE
                case 192:  // UNKNOWN_USER
                case 193:  // WRONG_PASSWORD
                case 497:  // ACCESS_DENIED
                    return false;
                default:
                    // Unknown code — fall through to message heuristic.
                    break;
            }
        }

        String msg = cause == null ? "" : String.valueOf(cause.getMessage()).toLowerCase();
        return msg.contains("timeout")
                || msg.contains("connection reset")
                || msg.contains("read timed out");
    }

    private static int extractClickHouseErrorCode(Throwable t) {
        if (t == null) return -1;
        String msg = t.getMessage();
        if (msg == null) return -1;
        // ClickHouse exception format: "Code: NNN. DB::Exception: ..."
        int codeIdx = msg.indexOf("Code:");
        if (codeIdx < 0) return -1;
        int start = codeIdx + 5;
        int end = start;
        while (end < msg.length() && Character.isWhitespace(msg.charAt(end))) end++;
        start = end;
        while (end < msg.length() && Character.isDigit(msg.charAt(end))) end++;
        if (end == start) return -1;
        try {
            return Integer.parseInt(msg.substring(start, end));
        } catch (NumberFormatException nfe) {
            return -1;
        }
    }

    private static Throwable unwrap(Throwable e) {
        Throwable cur = e;
        int depth = 0;
        while (cur != null && cur.getCause() != null && cur.getCause() != cur && depth++ < 8) {
            cur = cur.getCause();
        }
        return cur;
    }

    private InsertSettings buildInsertSettings(ClickHouseCommittable c) {
        InsertSettings settings = new InsertSettings();
        // MANDATORY for exactly-once: disable server-side async insert so the ack means durable.
        settings.setOption(ClientConfigProperties.ASYNC_OPERATIONS.getKey(), "false");
        // Make the commit idempotent. Same token on retry -> ClickHouse drops duplicate.
        settings.setOption("insert_deduplication_token", c.getDeduplicationToken());
        // Ensure dedup is on (defensive: default is 1 on replicated tables anyway).
        settings.serverSetting("insert_deduplicate", "1");
        return settings;
    }

    @Override
    public void close() throws Exception {
        if (chClient != null) {
            chClient.close();
            chClient = null;
        }
    }
}
