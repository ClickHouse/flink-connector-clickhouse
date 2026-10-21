package org.apache.flink.connector.clickhouse.token;

/** A short-lived ClickHouse Cloud JWT obtained by the delegation token provider. Immutable. */
public final class ClickHouseCredentials {

    private final String jwt;
    private final long validUntilMillis;

    public ClickHouseCredentials(String jwt, long validUntilMillis) {
        if (jwt == null || jwt.isEmpty()) {
            throw new IllegalArgumentException("ClickHouse JWT must not be empty");
        }
        this.jwt = jwt;
        this.validUntilMillis = validUntilMillis;
    }

    public String getJwt() {
        return jwt;
    }

    public long getValidUntilMillis() {
        return validUntilMillis;
    }

    /** Safe-to-log form of a secret: first 4 characters followed by an ellipsis. */
    public static String redact(String secret) {
        if (secret == null || secret.length() <= 4) {
            return "****";
        }
        return secret.substring(0, 4) + "…";
    }

    @Override
    public String toString() {
        return "ClickHouseCredentials{jwt=" + redact(jwt) + ", validUntilMillis=" + validUntilMillis + '}';
    }
}
