package org.apache.flink.connector.clickhouse.token;

/**
 * JVM-wide slot for the current ClickHouse JWT.
 *
 * <p>{@link ClickHouseDelegationTokenReceiver} writes it; the sink reads it before every batch.
 * Deliberately free of Flink types so that sink code without a Flink dependency can read it. It
 * works across classloaders only because this class is loaded from Flink's {@code lib/}
 * (package {@code org.apache.flink.*} is parent-first by default).
 */
public final class ClickHouseCredentialsHolder {

    private static volatile ClickHouseCredentials credentials;

    private ClickHouseCredentialsHolder() {}

    /** Current credentials, or {@code null} if none were received yet. */
    public static ClickHouseCredentials get() {
        return credentials;
    }

    public static void set(ClickHouseCredentials newCredentials) {
        if (newCredentials == null) {
            throw new IllegalArgumentException("credentials must not be null");
        }
        credentials = newCredentials;
    }

    /** Clears the slot. For tests. */
    public static void reset() {
        credentials = null;
    }
}
