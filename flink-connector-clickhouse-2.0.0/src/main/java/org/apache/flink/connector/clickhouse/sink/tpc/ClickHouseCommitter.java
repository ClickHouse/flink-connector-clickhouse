package org.apache.flink.connector.clickhouse.sink.tpc;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.data.ClickHouseFormat;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
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
 * Commits {@link ClickHouseCommittable}s to ClickHouse idempotently using Flink 2.0
 * {@link Committer} API.
 *
 * <p>Every commit is a blocking, synchronous HTTP insert with
 * {@code insert_deduplication_token} set. Retries re-send the same token, so ClickHouse
 * drops the second write if the first actually landed. Flink's 2PC protocol guarantees
 * the committable survives task failure in checkpoint state until commit succeeds.
 *
 * <p>Retry classification uses ClickHouse numeric error codes parsed from the exception
 * message. Transient infra errors (timeouts, too many parts, memory limit, IO errors)
 * trigger {@link CommitRequest#retryLater()}. Schema/auth errors fail the job via
 * {@link CommitRequest#signalFailedWithKnownReason}.
 */
public class ClickHouseCommitter implements Committer<ClickHouseCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseCommitter.class);
    private static final long DEFAULT_COMMIT_TIMEOUT_MS = 120_000L;
    private static final int CONNECTION_RESET_AFTER_FAILURES = 2;

    private final ClickHouseClientConfig clientConfig;
    private final long commitTimeoutMs;

    private transient Client chClient;
    private transient int consecutiveConnectionFailures;

    // Metrics — inspired by eBay Block Aggregator's fleet-wide observability. Registered
    // when the framework calls the CommitterInitContext-aware constructor.
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
     * Wire metrics from a {@link CommitterInitContext}. Safe to skip if the framework does
     * not provide a context; counters stay null and {@link #incr} silently no-ops.
     */
    public void registerMetrics(CommitterInitContext context) {
        if (context == null) return;
        MetricGroup group = context.metricGroup().addGroup("clickhouseCommitter");
        commitsSucceeded = group.counter("commitsSucceeded");
        commitsRetried = group.counter("commitsRetried");
        commitsFailedPermanent = group.counter("commitsFailedPermanent");
        recordsCommitted = group.counter("recordsCommitted");
        bytesCommitted = group.counter("bytesCommitted");
        writtenRowMismatches = group.counter("writtenRowMismatches");
        commitLatencyMs = group.histogram("commitLatencyMs",
                new DescriptiveStatisticsHistogram(1000));
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
            consecutiveConnectionFailures = 0;

            // Anomaly detection — analog of eBay ARV. writtenRows > recordCount should
            // never happen on a single-insert-per-committable. Non-zero mismatch = alert.
            long writtenRows = response.getWrittenRows();
            if (writtenRows > c.getRecordCount()) {
                LOG.error("Anomaly: writtenRows={} exceeds recordCount={} for token={}",
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

            if (isConnectionLevel(e)) {
                consecutiveConnectionFailures++;
                if (consecutiveConnectionFailures >= CONNECTION_RESET_AFTER_FAILURES) {
                    LOG.warn("Recycling ClickHouse client after {} consecutive connection failures",
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

    private InsertSettings buildInsertSettings(ClickHouseCommittable c) {
        InsertSettings settings = new InsertSettings();
        // MANDATORY for exactly-once: no server-side async inserts. Client ack == durable.
        settings.setOption(ClientConfigProperties.ASYNC_OPERATIONS.getKey(), "false");
        // Idempotency primitive — same token on retry → ClickHouse drops duplicate.
        settings.setOption("insert_deduplication_token", c.getDeduplicationToken());
        // Defensive — replicated tables have this on by default but harmless to set.
        settings.serverSetting("insert_deduplicate", "1");
        return settings;
    }

    private static boolean isRetriable(Throwable e) {
        Throwable cause = unwrap(e);
        if (cause instanceof TimeoutException) return true;
        if (cause instanceof java.net.SocketTimeoutException) return true;
        if (cause instanceof java.net.ConnectException) return true;
        if (cause instanceof java.io.IOException) return true;

        int code = extractClickHouseErrorCode(cause);
        if (code > 0) {
            switch (code) {
                case 76:   // CANNOT_OPEN_FILE
                case 209:  // SOCKET_TIMEOUT
                case 210:  // NETWORK_ERROR
                case 241:  // MEMORY_LIMIT_EXCEEDED
                case 242:  // TABLE_IS_READ_ONLY
                case 246:  // CORRUPTED_DATA
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
                    break;
            }
        }

        String msg = cause == null ? "" : String.valueOf(cause.getMessage()).toLowerCase();
        return msg.contains("timeout")
                || msg.contains("connection reset")
                || msg.contains("read timed out");
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

    private static int extractClickHouseErrorCode(Throwable t) {
        if (t == null) return -1;
        String msg = t.getMessage();
        if (msg == null) return -1;
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

    private void recycleClient() {
        // Drop the cached client held inside ClickHouseClientConfig so the next
        // createClient() call rebuilds the HTTP pool. Our local chClient reference
        // is also cleared so we re-fetch on next use.
        clientConfig.resetClient();
        chClient = null;
    }

    private static void incr(Counter c) {
        if (c != null) c.inc();
    }

    @Override
    public void close() throws Exception {
        recycleClient();
    }
}
