package org.apache.flink.connector.clickhouse.auth;

import java.io.Serializable;
import java.util.Objects;

/**
 * Sink-side copy of the current delegation token JWT. Contains no token-module types, so sink
 * classes holding it stay loadable when the token jar is absent (password mode).
 */
public final class ResolvedCredentials implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String jwt;
    private final long validUntilMillis;

    ResolvedCredentials(String jwt, long validUntilMillis) {
        this.jwt = jwt;
        this.validUntilMillis = validUntilMillis;
    }

    public String getJwt() {
        return jwt;
    }

    public long getValidUntilMillis() {
        return validUntilMillis;
    }

    public boolean isExpired(long nowMillis) {
        return validUntilMillis <= nowMillis;
    }

    boolean isSameToken(String otherJwt, long otherValidUntilMillis) {
        return validUntilMillis == otherValidUntilMillis && Objects.equals(jwt, otherJwt);
    }

    @Override
    public String toString() {
        String shown = jwt == null || jwt.length() <= 4 ? "****" : jwt.substring(0, 4) + "…";
        return "ResolvedCredentials{jwt=" + shown + ", validUntilMillis=" + validUntilMillis + '}';
    }
}
