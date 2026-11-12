package org.apache.flink.connector.clickhouse.sink.auth;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.clickhouse.auth.DelegationTokenAuth;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.connector.clickhouse.token.ClickHouseCredentials;
import org.apache.flink.connector.clickhouse.token.ClickHouseCredentialsHolder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * OAuth client-credentials end to end without Cloud: the provider fetches 20s JWTs from a fake identity provider
 * inside the MiniCluster, a JWT-verifying fake ClickHouse accepts inserts, and the sink must write
 * every row across several rotations with zero rejected requests, and fail cleanly when renewal is impossible.
 */
class ClickHouseDelegationTokenTests extends FlinkClusterTestsForAuth {

    private static final String ISSUER = "https://idp.test";
    private static final String AUDIENCE = "fake-clickhouse";
    private static final int ROWS = 700;

    private static FakeIdentityProvider idp;
    private static PublicKey publicKey;
    private static FakeJwtClickHouseServer clickhouse;

    @BeforeAll
    static void setUp() throws Exception {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(2048);
        KeyPair keyPair = gen.generateKeyPair();
        publicKey = keyPair.getPublic();
        idp = new FakeIdentityProvider(keyPair.getPrivate(), "test-key", ISSUER, AUDIENCE, "flink-it",
                Collections.singletonList("INSERT ON dt.events"), "flink", "s&cr3t", 20);
        clickhouse = new FakeJwtClickHouseServer(publicKey, ISSUER, AUDIENCE);

        // The provider runs inside the MiniCluster and is called at cluster start.
        Configuration conf = oauthProviderConfig(idp.tokenEndpoint(), "flink", "s&cr3t", null, null, null, "1s");
        put(conf, PROVIDER_PREFIX + "oauth.retry-interval", "2s"); // recover quickly after the outage test
        startCluster(conf);
    }

    @AfterAll
    static void stopFakes() {
        if (clickhouse != null) {
            clickhouse.close();
        }
        if (idp != null) {
            idp.close();
        }
    }

    @Test
    void sinkKeepsWritingAcrossJwtRotations() throws Exception {
        StreamExecutionEnvironment env = flinkEnv();
        // ~70s of steady writes: with 20s tokens Flink re-obtains about every 15s.
        env.fromSequence(1, ROWS)
                .map(i -> {
                    Thread.sleep(100);
                    return i + ",row-" + i;
                })
                .returns(Types.STRING)
                .sinkTo(csvSink(ClickHouseClientConfig.forDelegationToken(clickhouse.url(), "dt", "events"), 50));

        JobClient job = env.executeAsync("delegation-token-oauth");
        // Bounded source: the job FINISHES; a fatal auth failure (STOP_FLINK) would fail it here.
        job.getJobExecutionResult().get(180, TimeUnit.SECONDS);

        assertEquals(ROWS, clickhouse.rows());
        assertEquals(0, clickhouse.rejected(), "no request may be sent with an invalid or expired JWT");
        assertTrue(idp.issued() >= 3, "the provider must have re-obtained tokens, identity provider issued: " + idp.issued());
        assertTrue(clickhouse.distinctTokens() >= 3,
                "expected at least 2 rotations reaching ClickHouse, server saw JWTs: " + clickhouse.distinctTokens());
    }

    @Test
    void permanentlyRejectedTokenFailsTheJobInsteadOfRetryingForever() throws Exception {
        // Cloud JWT provider configured with a different audience: every request gets 516.
        try (FakeJwtClickHouseServer misconfigured = new FakeJwtClickHouseServer(
                KeyPairGenerator.getInstance("RSA").generateKeyPair().getPublic(), ISSUER, "other-audience")) {
            StreamExecutionEnvironment env = flinkEnv();
            env.fromSequence(1, 10).map(i -> i + ",row-" + i).returns(Types.STRING)
                    .sinkTo(csvSink(ClickHouseClientConfig.forDelegationToken(misconfigured.url(), "dt", "events"), 50));

            JobClient job = env.executeAsync("delegation-token-misconfigured");
            Exception e = assertThrows(Exception.class, () -> job.getJobExecutionResult().get(60, TimeUnit.SECONDS));
            assertFalse(e instanceof TimeoutException, "the sink must fail, not spin: " + e);
            assertTrue(causeChainContains(e, "rejected the delegation token"), "unexpected failure: " + e);
            assertTrue(misconfigured.rejected() <= DelegationTokenAuth.MAX_AUTH_FAILURES_PER_TOKEN + 1,
                    "bounded retries, server rejected: " + misconfigured.rejected());
        }
    }

    @Test
    void identityProviderOutageFailsTheJobWithTheExpiryMessageInsteadOfSpinning() throws Exception {
        // The 20s token expires with no renewal possible: fail with the expiry message, not a 516 retry loop.
        try (FakeJwtClickHouseServer server = new FakeJwtClickHouseServer(publicKey, ISSUER, AUDIENCE)) {
            ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(server.url(), "dt", "events");
            config.setDelegationTokenWaitTimeout(Duration.ofSeconds(3));
            StreamExecutionEnvironment env = flinkEnv();
            env.fromSequence(1, 1_000)
                    .map(i -> {
                        Thread.sleep(100);
                        return i + ",row-" + i;
                    })
                    .returns(Types.STRING)
                    .sinkTo(csvSink(config, 50));

            idp.setAvailable(false);
            try {
                JobClient job = env.executeAsync("delegation-token-idp-outage");
                Exception e = assertThrows(Exception.class, () -> job.getJobExecutionResult().get(90, TimeUnit.SECONDS));
                assertFalse(e instanceof TimeoutException, "the sink must fail, not spin: " + e);
                assertTrue(causeChainContains(e, "no newer one arrived"), "unexpected failure: " + e);
                assertTrue(server.rejected() <= 10, "the expired JWT was re-sent in a loop, server rejected: "
                        + server.rejected());
            } finally {
                idp.setAvailable(true);
                awaitFreshToken();
            }
        }
    }

    /** Leaves the cluster with a renewed token for the other tests. */
    private static void awaitFreshToken() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            ClickHouseCredentials c = ClickHouseCredentialsHolder.get();
            if (c != null && c.getValidUntilMillis() > System.currentTimeMillis() + 5_000) {
                return;
            }
            Thread.sleep(200);
        }
        fail("the provider did not renew the token after the identity provider recovered");
    }

    private static boolean causeChainContains(Throwable t, String text) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c.getMessage() != null && c.getMessage().contains(text)) {
                return true;
            }
        }
        return false;
    }
}
