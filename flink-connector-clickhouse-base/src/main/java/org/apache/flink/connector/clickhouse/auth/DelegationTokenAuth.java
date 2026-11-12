package org.apache.flink.connector.clickhouse.auth;

import com.clickhouse.client.api.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;

/** Per-client delegation-token state: the JWT in use and the auth-failure budget. */
public final class DelegationTokenAuth {

    private static final Logger LOG = LoggerFactory.getLogger(DelegationTokenAuth.class);

    /** Auth failures tolerated per JWT, counted at most once per auth-retry backoff. */
    public static final int MAX_AUTH_FAILURES_PER_TOKEN = 3;

    private static final Duration DEFAULT_RETRY_BACKOFF = Duration.ofSeconds(1);

    private final Duration waitTimeout;
    private final Duration retryBackoff;
    private volatile ResolvedCredentials applied;
    private ResolvedCredentials failureToken;
    private int failures;
    private long failureCountedAtNanos;

    public DelegationTokenAuth(Duration waitTimeout) {
        this(waitTimeout, DEFAULT_RETRY_BACKOFF);
    }

    DelegationTokenAuth(Duration waitTimeout, Duration retryBackoff) {
        this.waitTimeout = Objects.requireNonNull(waitTimeout, "waitTimeout");
        this.retryBackoff = Objects.requireNonNull(retryBackoff, "retryBackoff");
    }

    /**
     * Waits up to the wait timeout for the first unexpired JWT, which the client must be built with.
     *
     * @throws IllegalStateException if none arrived in time
     */
    public ResolvedCredentials awaitFirst() {
        try {
            applied = DelegationTokenBridge.await(waitTimeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for ClickHouse delegation token", e);
        }
        return applied;
    }

    /** The JWT last applied to the client, or {@code null} before {@link #awaitFirst()}. */
    public ResolvedCredentials getAppliedCredentials() {
        return applied;
    }

    /** Applies a newer JWT to the client, if one arrived since the last call; one volatile read otherwise. */
    public void refresh(Client client) {
        ResolvedCredentials current = DelegationTokenBridge.current();
        if (current == null || current == applied) {
            return;
        }
        apply(client, current);
    }

    /**
     * The JWT for the next insert, never an expired or just-rejected one if a newer one arrives in time.
     *
     * @throws IllegalStateException if the JWT expired and no newer one arrived within the wait timeout
     */
    public ResolvedCredentials awaitUsable(Client client) {
        try {
            refresh(client);
            long backoffNanos;
            synchronized (this) {
                backoffNanos = failureToken == applied && failures > 0
                        ? failureCountedAtNanos + retryBackoff.toNanos() - System.nanoTime() : 0;
            }
            if (backoffNanos > 0) {
                DelegationTokenBridge.awaitOther(applied, Duration.ofNanos(backoffNanos));
                refresh(client);
            }
            if (applied.isExpired(System.currentTimeMillis())) {
                apply(client, DelegationTokenBridge.await(waitTimeout));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for ClickHouse delegation token", e);
        }
        return applied;
    }

    private void apply(Client client, ResolvedCredentials credentials) {
        client.updateBearerToken(credentials.getJwt());
        applied = credentials;
        LOG.info("Applied re-obtained ClickHouse delegation token {}", credentials);
    }

    /**
     * Called when ClickHouse rejected the JWT a request was sent with (516/720). Returns {@code true}
     * if the batch should be retried: the token had expired (renewal is late), a newer token arrived
     * since the request was sent (expiry race), or this token has failed at most
     * {@link #MAX_AUTH_FAILURES_PER_TOKEN} times, counted at most once per auth-retry backoff.
     * {@code false} means the token is rejected permanently (e.g. wrong audience in the Cloud JWT
     * provider), so the job should fail with a clear error instead of retrying forever.
     */
    public synchronized boolean onAuthFailure(ResolvedCredentials usedForRequest) {
        if (usedForRequest != null && usedForRequest.isExpired(System.currentTimeMillis())) {
            // Rejected because it expired (renewal is late, e.g. identity provider down), not misconfigured.
            return true;
        }
        ResolvedCredentials latest = DelegationTokenBridge.current();
        if (latest != null && latest != usedForRequest) {
            failureToken = latest;
            failures = 0;
            return true;
        }
        long now = System.nanoTime();
        if (usedForRequest != failureToken) {
            failureToken = usedForRequest;
            failures = 0;
        } else if (failures > 0 && now - failureCountedAtNanos < retryBackoff.toNanos()) {
            return true;
        }
        failureCountedAtNanos = now;
        failures++;
        return failures <= MAX_AUTH_FAILURES_PER_TOKEN;
    }

    /** Resets the auth-failure budget after a successful insert. */
    public synchronized void onWriteSuccess() {
        failureToken = null;
        failures = 0;
    }
}
