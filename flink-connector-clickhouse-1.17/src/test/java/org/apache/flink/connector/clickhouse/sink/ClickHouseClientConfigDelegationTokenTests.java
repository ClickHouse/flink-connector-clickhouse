package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.client.api.Client;
import org.apache.flink.connector.clickhouse.token.ClickHouseCredentials;
import org.apache.flink.connector.clickhouse.token.ClickHouseCredentialsHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/** No ClickHouse server needed: client-v2 builds clients lazily and these tests never send a request. */
class ClickHouseClientConfigDelegationTokenTests {

    // Port 1 is never listening: proves no connection is attempted while building the config.
    private static final String UNREACHABLE_URL = "https://127.0.0.1:1/";

    @AfterEach
    void reset() {
        ClickHouseCredentialsHolder.reset();
    }

    private static void publish(String jwt) {
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials(jwt, System.currentTimeMillis() + 60_000));
    }

    @Test
    void forDelegationTokenRequiresHttpsExceptForLoopback() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ClickHouseClientConfig.forDelegationToken("http://my-service.clickhouse.cloud:8123/", "db", "events"));
        assertTrue(e.getMessage().contains("HTTPS"), e.getMessage());
        assertTrue(ClickHouseClientConfig.forDelegationToken("http://127.0.0.1:8123/", "db", "events").isDelegationTokenMode());
        assertTrue(ClickHouseClientConfig.forDelegationToken("http://localhost:8123/", "db", "events").isDelegationTokenMode());
    }

    @Test
    void forDelegationTokenDoesNotPingTheServer() {
        long start = System.nanoTime();
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        assertTrue(config.isDelegationTokenMode());
        assertTrue(Duration.ofNanos(System.nanoTime() - start).toMillis() < 1000, "constructor must not ping/retry");
    }

    @Test
    void createClientUsesTheJwtFromTheHolder() {
        publish("eyJ.jwt1.sig");
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        assertNotNull(config.createClient());
        assertEquals("eyJ.jwt1.sig", config.getAppliedCredentials().getJwt());
    }

    @Test
    void refreshSwapsTheJwtOnTheSameClient() {
        publish("eyJ.jwt1.sig");
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        Client client = config.createClient();

        config.refreshCredentialsIfChanged();
        assertEquals("eyJ.jwt1.sig", config.getAppliedCredentials().getJwt(), "no change, no swap");

        publish("eyJ.jwt2.sig");
        config.refreshCredentialsIfChanged();
        assertSame(client, config.createClient());
        assertEquals("eyJ.jwt2.sig", config.getAppliedCredentials().getJwt());
    }

    @Test
    void createClientTimesOutWithoutToken() {
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        config.setDelegationTokenWaitTimeout(Duration.ofMillis(200));
        IllegalStateException e = assertThrows(IllegalStateException.class, config::createClient);
        assertTrue(e.getMessage().contains("security.delegation.token.provider.clickhouse.oauth.token-endpoint"), e.getMessage());
    }

    @Test
    void refreshIsNoOpBeforeClientExists() {
        publish("eyJ.jwt1.sig");
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        config.refreshCredentialsIfChanged();
        assertNull(config.getAppliedCredentials());
    }

    @Test
    void sameTokenRejectedRepeatedlyExhaustsTheRetryBudget() {
        publish("eyJ.jwt1.sig");
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        config.createClient();
        for (int i = 0; i < ClickHouseClientConfig.MAX_AUTH_FAILURES_PER_TOKEN; i++) {
            assertTrue(config.onAuthFailure(config.getAppliedCredentials()), "retry " + i);
        }
        assertFalse(config.onAuthFailure(config.getAppliedCredentials()), "budget exhausted for the same token");
    }

    @Test
    void newerTokenAllowsRetryAndResetsTheBudget() {
        publish("eyJ.jwt1.sig");
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        config.createClient();
        org.apache.flink.connector.clickhouse.auth.ResolvedCredentials used = config.getAppliedCredentials();
        for (int i = 0; i < ClickHouseClientConfig.MAX_AUTH_FAILURES_PER_TOKEN; i++) {
            config.onAuthFailure(used);
        }
        publish("eyJ.jwt2.sig"); // a newer token arrived: the failed request used a stale one
        assertTrue(config.onAuthFailure(used));
        for (int i = 0; i < ClickHouseClientConfig.MAX_AUTH_FAILURES_PER_TOKEN; i++) {
            config.refreshCredentialsIfChanged();
            assertTrue(config.onAuthFailure(config.getAppliedCredentials()), "fresh budget for the new token, retry " + i);
        }
    }

    @Test
    void successfulWriteResetsTheBudget() {
        publish("eyJ.jwt1.sig");
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        config.createClient();
        for (int i = 0; i < ClickHouseClientConfig.MAX_AUTH_FAILURES_PER_TOKEN; i++) {
            config.onAuthFailure(config.getAppliedCredentials());
        }
        config.onWriteSuccess();
        assertTrue(config.onAuthFailure(config.getAppliedCredentials()));
    }

    @Test
    void expiredTokenRejectionsNeverExhaustTheBudget() throws Exception {
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("eyJ.short.sig", System.currentTimeMillis() + 300));
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        config.createClient();
        org.apache.flink.connector.clickhouse.auth.ResolvedCredentials used = config.getAppliedCredentials();
        Thread.sleep(400); // the token expires while renewal is late (identity provider down)
        for (int i = 0; i < 10; i++) {
            assertTrue(config.onAuthFailure(used), "rejection of an expired token is never permanent, attempt " + i);
        }
    }

    @Test
    void reBroadcastOfTheSameJwtDoesNotResetTheBudget() {
        long validUntil = System.currentTimeMillis() + 60_000;
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("eyJ.jwt1.sig", validUntil));
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        config.createClient();
        org.apache.flink.connector.clickhouse.auth.ResolvedCredentials used = config.getAppliedCredentials();
        for (int i = 0; i < ClickHouseClientConfig.MAX_AUTH_FAILURES_PER_TOKEN; i++) {
            assertTrue(config.onAuthFailure(used));
        }
        // The JobManager re-sends its last good token after an identity provider failure: not a new token.
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("eyJ.jwt1.sig", validUntil));
        config.refreshCredentialsIfChanged();
        assertSame(used, config.getAppliedCredentials());
        assertFalse(config.onAuthFailure(used), "budget still exhausted: the same JWT keeps being rejected");
    }

    @Test
    void createClientWaitsForAFreshTokenInsteadOfUsingAnExpiredOne() {
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("eyJ.stale.sig", System.currentTimeMillis() - 1_000));
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        config.setDelegationTokenWaitTimeout(Duration.ofMillis(200));
        IllegalStateException e = assertThrows(IllegalStateException.class, config::createClient);
        assertTrue(e.getMessage().contains("expired at"), e.getMessage());
    }

    @Test
    void configSurvivesJavaSerialization() throws Exception {
        publish("SECRET-JWT-7f3a9c");
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        config.createClient(); // populates transient state that must not travel

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(config);
        }
        assertFalse(new String(bytes.toByteArray(), StandardCharsets.ISO_8859_1).contains("SECRET-JWT-7f3a9c"),
                "the JWT must not be serialized with the config");
        ClickHouseClientConfig copy;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            copy = (ClickHouseClientConfig) in.readObject();
        }
        assertTrue(copy.isDelegationTokenMode());
        assertNull(copy.getAppliedCredentials());
        assertNotNull(copy.createClient());
        assertEquals("SECRET-JWT-7f3a9c", copy.getAppliedCredentials().getJwt(), "re-resolved from the holder on the TaskManager side");
    }
}
