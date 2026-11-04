package org.apache.flink.connector.clickhouse.token;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Obtains short-lived ClickHouse Cloud JWTs on the JobManager with the OAuth2 client-credentials
 * grant. Flink decides when to call {@link #obtainDelegationTokens()}; this class sends the
 * request to the identity provider. Inactive unless
 * {@code security.delegation.token.provider.clickhouse.oauth.token-endpoint} is set.
 */
public class ClickHouseDelegationTokenProvider implements DelegationTokenProvider {

    public static final String SERVICE_NAME = "clickhouse";

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseDelegationTokenProvider.class);

    private final Clock clock;
    private final Duration initialBackoff;
    private ProviderConfig config;
    private OAuthClientCredentialsSource source;
    private ClickHouseCredentials lastGood; // Flink calls obtainDelegationTokens from one thread at a time

    public ClickHouseDelegationTokenProvider() {
        this(Clock.systemUTC(), Duration.ofSeconds(1));
    }

    /** Tests: controllable clock and backoff between in-call retries. */
    ClickHouseDelegationTokenProvider(Clock clock, Duration initialBackoff) {
        this.clock = clock;
        this.initialBackoff = initialBackoff;
    }

    @Override
    public String serviceName() {
        return SERVICE_NAME;
    }

    @Override
    public void init(Configuration configuration) {
        config = ProviderConfig.parse(configuration.toMap());
        if (!config.isActive()) {
            LOG.debug("ClickHouse delegation token provider is not configured ({}oauth.token-endpoint not set); inactive",
                    ProviderConfig.PREFIX);
            return;
        }
        HttpClient http = HttpClient.newBuilder().connectTimeout(config.getConnectTimeout()).build();
        source = new OAuthClientCredentialsSource(config.getTokenEndpoint(), config.getClientId(),
                config.getClientSecret(), config.getScope(), config.getAudience(), config.getRequestTimeout(), initialBackoff,
                http, clock);
        config.insecureEndpointWarning().ifPresent(LOG::warn);
        LOG.info("ClickHouse delegation token provider initialized: token endpoint {}",
                ProviderConfig.displayable(config.getTokenEndpoint()));
    }

    @Override
    public boolean delegationTokensRequired() {
        return config != null && config.isActive();
    }

    /**
     * Never throws once initialized. On one provider exception Flink fails the update for every
     * provider and retries only after security.delegation.tokens.renewal.retry.backoff (default 1h),
     * longer than the margin a 60-90 minute JWT leaves. Instead, re-send the last good token (or
     * "none yet") and use validUntil, which only schedules the next call, to retry after
     * oauth.retry-interval.
     */
    @Override
    public ObtainedDelegationTokens obtainDelegationTokens() throws Exception {
        ClickHouseCredentials credentials;
        try {
            credentials = source.obtain();
        } catch (InterruptedException e) {
            throw e; // JobManager shutting down
        } catch (Exception e) {
            return retrySoon(e);
        }
        lastGood = credentials;
        LOG.info("Obtained ClickHouse JWT {} valid until {}", ClickHouseCredentials.redact(credentials.getJwt()),
                Instant.ofEpochMilli(credentials.getValidUntilMillis()));

        List<String> problems = OAuthClientCredentialsSource.cloudCompatibilityProblems(credentials.getJwt());
        if (!problems.isEmpty()) {
            LOG.warn("The identity provider's access token will likely be rejected by ClickHouse Cloud: {}. "
                    + "Fix the identity provider app or the Cloud JWT provider settings.", String.join("; ", problems));
        }
        return new ObtainedDelegationTokens(ClickHouseCredentialsCodec.encode(credentials),
                Optional.of(credentials.getValidUntilMillis()));
    }

    private ObtainedDelegationTokens retrySoon(Exception cause) {
        long now = clock.millis();
        Duration retry = config.getRetryInterval();
        double ratio = config.getRenewalRatio() > 0 ? config.getRenewalRatio() : 1.0;
        // Flink waits ratio * (validUntil - now) before the next call; divide so the wait is the retry interval.
        Optional<Long> retryAt = Optional.of(now + Math.round(retry.toMillis() / ratio));
        ClickHouseCredentials last = lastGood;
        if (last == null) {
            LOG.error("Could not obtain a ClickHouse JWT from the identity provider; retrying in {}. "
                    + "The ClickHouse sink cannot write until this succeeds.", retry, cause);
            return new ObtainedDelegationTokens(new byte[0], retryAt);
        }
        if (last.getValidUntilMillis() > now) {
            LOG.warn("Could not re-obtain the ClickHouse JWT; keeping {} (valid until {}), retrying in {}",
                    ClickHouseCredentials.redact(last.getJwt()), Instant.ofEpochMilli(last.getValidUntilMillis()),
                    retry, cause);
        } else {
            LOG.error("Could not re-obtain the ClickHouse JWT and the last one expired at {}; ClickHouse writes fail "
                    + "until the identity provider recovers. Retrying in {}",
                    Instant.ofEpochMilli(last.getValidUntilMillis()), retry, cause);
        }
        return new ObtainedDelegationTokens(ClickHouseCredentialsCodec.encode(last), retryAt);
    }
}
