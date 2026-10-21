package org.apache.flink.connector.clickhouse.auth;

import org.apache.flink.connector.clickhouse.token.ClickHouseCredentials;
import org.apache.flink.connector.clickhouse.token.ClickHouseCredentialsHolder;

import java.time.Duration;
import java.time.Instant;

/**
 * The only sink class that touches flink-connector-clickhouse-token. Loaded only in
 * {@link AuthMode#DELEGATION_TOKEN} mode, so password-mode jobs never need the token jar.
 */
public final class DelegationTokenBridge {

    static final String MISSING_JAR_MESSAGE =
            "ClickHouse delegation token auth requires flink-connector-clickhouse-token.jar in Flink's lib/ directory "
                    + "on the JobManager and every TaskManager";

    static final String TIMEOUT_MESSAGE =
            "No ClickHouse delegation token received within %s. Check that (1) "
                    + "security.delegation.token.provider.clickhouse.oauth.token-endpoint (with client id/secret) is set "
                    + "in the Flink configuration and the JobManager log shows 'Obtained ClickHouse JWT', (2) "
                    + "flink-connector-clickhouse-token.jar is in lib/ on the JobManager and every TaskManager, and (3) "
                    + "classloader.parent-first-patterns.default still contains org.apache.flink.";

    static final String EXPIRED_MESSAGE =
            "The latest ClickHouse delegation token expired at %s and no newer one arrived within %s: the JobManager "
                    + "cannot renew it. Check the JobManager log for 'Could not re-obtain the ClickHouse JWT' "
                    + "(identity provider errors).";

    private static final long POLL_MILLIS = 100;

    private static ClickHouseCredentials lastSource;
    private static ResolvedCredentials lastResolved;

    private DelegationTokenBridge() {}

    /** Current JWT or {@code null}; the same instance is returned until a new token arrives. */
    public static synchronized ResolvedCredentials current() {
        ClickHouseCredentials source;
        try {
            source = ClickHouseCredentialsHolder.get();
        } catch (NoClassDefFoundError e) {
            throw new IllegalStateException(MISSING_JAR_MESSAGE, e);
        }
        if (source == null) {
            return null;
        }
        if (source != lastSource) {
            lastSource = source;
            // The provider re-sends its last good token while the identity provider is down: not a new token.
            if (lastResolved == null || !lastResolved.isSameToken(source.getJwt(), source.getValidUntilMillis())) {
                lastResolved = new ResolvedCredentials(source.getJwt(), source.getValidUntilMillis());
            }
        }
        return lastResolved;
    }

    /** Waits up to {@code timeout} for an unexpired token; an expired one counts as missing. */
    public static ResolvedCredentials await(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            ResolvedCredentials credentials = current();
            if (credentials != null && !credentials.isExpired(System.currentTimeMillis())) {
                return credentials;
            }
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException(credentials == null
                        ? String.format(TIMEOUT_MESSAGE, timeout)
                        : String.format(EXPIRED_MESSAGE, Instant.ofEpochMilli(credentials.getValidUntilMillis()), timeout));
            }
            Thread.sleep(POLL_MILLIS);
        }
    }
}
