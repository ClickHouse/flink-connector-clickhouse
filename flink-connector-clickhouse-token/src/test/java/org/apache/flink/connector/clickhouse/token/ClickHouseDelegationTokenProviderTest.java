package org.apache.flink.connector.clickhouse.token;

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.core.security.token.DelegationTokenReceiver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ServiceLoader;

import static org.apache.flink.connector.clickhouse.token.ProviderConfig.PREFIX;
import static org.junit.jupiter.api.Assertions.*;

class ClickHouseDelegationTokenProviderTest {

    private StubHttpServer idp;

    @BeforeEach
    void start() throws IOException {
        idp = new StubHttpServer();
    }

    @AfterEach
    void stop() {
        idp.close();
        ClickHouseCredentialsHolder.reset();
    }

    private static void put(Configuration conf, String key, String value) {
        conf.set(ConfigOptions.key(key).stringType().noDefaultValue(), value);
    }

    private static final String TOKEN_1 = "{\"access_token\":\"eyJ.first.sig\",\"expires_in\":3600}";
    private static final String TOKEN_2 = "{\"access_token\":\"eyJ.second.sig\",\"expires_in\":5400}";
    private final MutableClock clock = new MutableClock(Instant.ofEpochSecond(1_000_000L));

    private Configuration oauthConf() {
        Configuration conf = new Configuration();
        put(conf, PREFIX + "oauth.token-endpoint", idp.uri("/token").toString());
        put(conf, PREFIX + "oauth.client-id", "flink");
        put(conf, PREFIX + "oauth.client-secret", "s3cret");
        return conf;
    }

    /** Controlled clock, no backoff between in-call retries. */
    private ClickHouseDelegationTokenProvider provider(Configuration conf) {
        ClickHouseDelegationTokenProvider provider = new ClickHouseDelegationTokenProvider(clock, Duration.ZERO);
        provider.init(conf);
        return provider;
    }

    private long now() {
        return clock.millis();
    }

    @Test
    void failureBeforeAnyTokenReturnsNoTokenAndRetriesSoon() throws Exception {
        idp.respond(503, "{}");
        DelegationTokenProvider.ObtainedDelegationTokens obtained = provider(oauthConf()).obtainDelegationTokens();
        assertEquals(0, obtained.getTokens().length, "empty bytes mean 'no token yet'");
        // Flink waits ratio * (validUntil - now): 0.75 * 80s = the 60s retry interval.
        assertEquals(now() + 80_000L, obtained.getValidUntil().orElseThrow(AssertionError::new));
    }

    @Test
    void failureKeepsTheLastGoodTokenAndRetriesSoon() throws Exception {
        ClickHouseDelegationTokenProvider provider = provider(oauthConf());
        idp.respond(200, TOKEN_1);
        provider.obtainDelegationTokens();

        idp.respond(503, "{}");
        clock.advance(Duration.ofMinutes(45));
        DelegationTokenProvider.ObtainedDelegationTokens obtained = provider.obtainDelegationTokens();
        ClickHouseCredentials resent = ClickHouseCredentialsCodec.decode(obtained.getTokens());
        assertEquals("eyJ.first.sig", resent.getJwt());
        assertEquals((1_000_000L + 3600L) * 1000L, resent.getValidUntilMillis(), "real expiry travels unchanged");
        assertEquals(now() + 80_000L, obtained.getValidUntil().orElseThrow(AssertionError::new));
    }

    @Test
    void failureAfterTheLastTokenExpiredDoesNotThrow() throws Exception {
        ClickHouseDelegationTokenProvider provider = provider(oauthConf());
        idp.respond(200, TOKEN_1);
        provider.obtainDelegationTokens();

        idp.respond(503, "{}");
        clock.advance(Duration.ofHours(2));
        DelegationTokenProvider.ObtainedDelegationTokens obtained = provider.obtainDelegationTokens();
        assertEquals("eyJ.first.sig", ClickHouseCredentialsCodec.decode(obtained.getTokens()).getJwt());
        assertEquals(now() + 80_000L, obtained.getValidUntil().orElseThrow(AssertionError::new));
    }

    @Test
    void clientErrorsAlsoNeverThrow() throws Exception {
        idp.respond(401, "{\"error\":\"invalid_client\"}");
        DelegationTokenProvider.ObtainedDelegationTokens obtained = provider(oauthConf()).obtainDelegationTokens();
        assertEquals(0, obtained.getTokens().length);
        assertEquals(now() + 80_000L, obtained.getValidUntil().orElseThrow(AssertionError::new));
        assertEquals(1, idp.requests.get(), "not retried inside the call");
    }

    @Test
    void recoveryResumesTheNormalSchedule() throws Exception {
        ClickHouseDelegationTokenProvider provider = provider(oauthConf());
        idp.respond(200, TOKEN_1);
        provider.obtainDelegationTokens();
        idp.respond(503, "{}");
        provider.obtainDelegationTokens();

        idp.respond(200, TOKEN_2);
        clock.advance(Duration.ofMinutes(1));
        DelegationTokenProvider.ObtainedDelegationTokens obtained = provider.obtainDelegationTokens();
        assertEquals("eyJ.second.sig", ClickHouseCredentialsCodec.decode(obtained.getTokens()).getJwt());
        assertEquals(now() + 5_400_000L, obtained.getValidUntil().orElseThrow(AssertionError::new));
    }

    @Test
    void retryIntervalIsCompensatedForTheRenewalRatio() throws Exception {
        Configuration conf = oauthConf();
        put(conf, PREFIX + "oauth.retry-interval", "30s");
        put(conf, "security.delegation.tokens.renewal.time-ratio", "0.5");
        idp.respond(503, "{}");
        DelegationTokenProvider.ObtainedDelegationTokens obtained = provider(conf).obtainDelegationTokens();
        assertEquals(now() + 60_000L, obtained.getValidUntil().orElseThrow(AssertionError::new), "0.5 * 60s = 30s");
    }

    @Test
    void transientErrorsAreRetriedBeforeFallingBack() throws Exception {
        idp.enqueue(503, "{}");
        idp.enqueue(503, "{}");
        idp.respond(200, TOKEN_1);
        DelegationTokenProvider.ObtainedDelegationTokens obtained = provider(oauthConf()).obtainDelegationTokens();
        assertEquals("eyJ.first.sig", ClickHouseCredentialsCodec.decode(obtained.getTokens()).getJwt());
        assertEquals(3, idp.requests.get());
    }

    @Test
    void receiverIgnoresTheNoTokenMarker() {
        ClickHouseDelegationTokenReceiver receiver = new ClickHouseDelegationTokenReceiver();
        receiver.onNewTokensObtained(ClickHouseCredentialsCodec.encode(new ClickHouseCredentials("jwt-1", 5L)));
        receiver.onNewTokensObtained(new byte[0]);
        assertEquals("jwt-1", ClickHouseCredentialsHolder.get().getJwt());
    }

    @Test
    void unconfiguredProviderIsInactive() throws Exception {
        ClickHouseDelegationTokenProvider provider = new ClickHouseDelegationTokenProvider();
        provider.init(new Configuration());
        assertEquals("clickhouse", provider.serviceName());
        assertFalse(provider.delegationTokensRequired());
    }

    @Test
    void obtainsEncodedCredentialsFromTheIdentityProvider() throws Exception {
        idp.respond(200, "{\"access_token\":\"eyJ.payload.sig\",\"expires_in\":3600}");
        Configuration conf = new Configuration();
        put(conf, PREFIX + "oauth.token-endpoint", idp.uri("/token").toString());
        put(conf, PREFIX + "oauth.client-id", "flink");
        put(conf, PREFIX + "oauth.client-secret", "s3cret");

        ClickHouseDelegationTokenProvider provider = new ClickHouseDelegationTokenProvider();
        provider.init(conf);
        assertTrue(provider.delegationTokensRequired());

        long before = System.currentTimeMillis();
        DelegationTokenProvider.ObtainedDelegationTokens obtained = provider.obtainDelegationTokens();
        long validUntil = obtained.getValidUntil().orElseThrow(AssertionError::new);
        assertTrue(validUntil >= before + 3_600_000L && validUntil <= System.currentTimeMillis() + 3_600_000L);
        assertEquals("eyJ.payload.sig", ClickHouseCredentialsCodec.decode(obtained.getTokens()).getJwt());
        assertEquals("/token", idp.lastPath);
    }

    @Test
    void configuredRequestTimeoutIsApplied() throws Exception {
        idp.respond(200, TOKEN_1);
        idp.stallBody(60_000);
        Configuration conf = oauthConf();
        put(conf, PREFIX + "oauth.request-timeout", "500ms");

        long start = System.nanoTime();
        DelegationTokenProvider.ObtainedDelegationTokens obtained = provider(conf).obtainDelegationTokens();
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertEquals(0, obtained.getTokens().length);
        // The stub serves one exchange at a time and the first one stays stalled, so later attempts
        // time out waiting for headers: count attempts by time, 4 x 500ms.
        assertTrue(elapsedMillis >= 4 * 500 - 100 && elapsedMillis < 10_000, "took " + elapsedMillis + " ms");
    }

    @Test
    void invalidConfigurationFailsInit() {
        Configuration conf = new Configuration();
        put(conf, PREFIX + "oauth.token-endpoint", "https://idp.example.com/token");
        ClickHouseDelegationTokenProvider provider = new ClickHouseDelegationTokenProvider();
        assertThrows(IllegalConfigurationException.class, () -> provider.init(conf));
    }

    @Test
    void receiverPublishesCredentialsToHolder() throws Exception {
        ClickHouseDelegationTokenReceiver receiver = new ClickHouseDelegationTokenReceiver();
        receiver.init(new Configuration());
        assertEquals("clickhouse", receiver.serviceName());
        receiver.onNewTokensObtained(ClickHouseCredentialsCodec.encode(new ClickHouseCredentials("jwt-1", 5L)));
        assertEquals("jwt-1", ClickHouseCredentialsHolder.get().getJwt());
    }

    @Test
    void receiverKeepsPreviousCredentialsOnBadPayload() throws Exception {
        ClickHouseDelegationTokenReceiver receiver = new ClickHouseDelegationTokenReceiver();
        receiver.onNewTokensObtained(ClickHouseCredentialsCodec.encode(new ClickHouseCredentials("jwt-1", 5L)));
        assertThrows(IllegalArgumentException.class, () -> receiver.onNewTokensObtained(new byte[] {9, 1}));
        assertEquals("jwt-1", ClickHouseCredentialsHolder.get().getJwt());
    }

    @Test
    void serviceLoaderFindsProviderAndReceiver() {
        boolean provider = false;
        for (DelegationTokenProvider p : ServiceLoader.load(DelegationTokenProvider.class)) {
            provider |= p instanceof ClickHouseDelegationTokenProvider;
        }
        boolean receiver = false;
        for (DelegationTokenReceiver r : ServiceLoader.load(DelegationTokenReceiver.class)) {
            receiver |= r instanceof ClickHouseDelegationTokenReceiver;
        }
        assertTrue(provider, "provider registered in META-INF/services");
        assertTrue(receiver, "receiver registered in META-INF/services");
    }
}
