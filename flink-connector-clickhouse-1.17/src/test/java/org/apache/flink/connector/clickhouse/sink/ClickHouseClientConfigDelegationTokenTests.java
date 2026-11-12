package org.apache.flink.connector.clickhouse.sink;

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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** No ClickHouse server needed: client-v2 builds clients lazily and these tests never send an insert. */
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
        assertEquals("eyJ.jwt1.sig", config.getDelegationTokenAuth().getAppliedCredentials().getJwt());
    }

    @Test
    void createClientTimesOutWithoutToken() {
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        config.setDelegationTokenWaitTimeout(Duration.ofMillis(200));
        IllegalStateException e = assertThrows(IllegalStateException.class, config::createClient);
        assertTrue(e.getMessage().contains("security.delegation.token.provider.clickhouse.oauth.token-endpoint"), e.getMessage());
    }

    @Test
    void noTokenStateBeforeTheClientExists() {
        publish("eyJ.jwt1.sig");
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        assertNull(config.getDelegationTokenAuth());
    }

    @Test
    void passwordModeHasNoTokenState() {
        ClickHouseClientConfig config = new ClickHouseClientConfig(UNREACHABLE_URL, "default", "pw", "db", "events");
        config.createClient();
        assertNull(config.getDelegationTokenAuth());
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
    void verifyConnectivitySkipsTheCheckForDelegationTokens() {
        // ClickHouseAsyncSinkBuilder.build() calls this on the job driver, which never has a token.
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        long start = System.nanoTime();
        config.verifyConnectivity();
        assertTrue(Duration.ofNanos(System.nanoTime() - start).toMillis() < 1000, "must not try to connect");
    }

    @Test
    void delegationTokenConfigRejectsCredentialOptions() {
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        for (String key : new String[] {"user", "password", "access_token", "bearer_token", "ssl_authentication",
                "http_header_AUTHORIZATION", "http_header_Authorization"}) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> config.setOptions(Map.of(key, "x")), key);
            assertTrue(e.getMessage().contains(key), e.getMessage());
        }
        config.setOptions(Map.of("socket_timeout", "1234")); // non-credential options still pass through
    }

    @Test
    void copyPreservesDelegationTokenMode() {
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        config.setOptions(Map.of("socket_timeout", "1234"));
        config.setDelegationTokenWaitTimeout(Duration.ofMillis(200));
        ClickHouseClientConfig copy = config.copy();

        assertTrue(copy.isDelegationTokenMode(), "a copy must not fall back to password auth");
        long start = System.nanoTime();
        assertThrows(IllegalStateException.class, copy::createClient, "no token published: the copy waits, then fails");
        assertTrue(Duration.ofNanos(System.nanoTime() - start).toMillis() < 5000, "the 200ms wait timeout was copied");

        publish("eyJ.jwt1.sig");
        assertEquals("1234", copy.createClient().getConfiguration().get("socket_timeout"), "options were copied");
        assertEquals("eyJ.jwt1.sig", copy.getDelegationTokenAuth().getAppliedCredentials().getJwt());
    }

    @Test
    void createPlanningClientRejectsDelegationTokens() {
        ClickHouseClientConfig config = ClickHouseClientConfig.forDelegationToken(UNREACHABLE_URL, "db", "events");
        UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class,
                () -> config.createPlanningClient(Map.of()));
        assertTrue(e.getMessage().contains("Table API"), e.getMessage());
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
        assertNull(copy.getDelegationTokenAuth());
        assertNotNull(copy.createClient());
        assertEquals("SECRET-JWT-7f3a9c", copy.getDelegationTokenAuth().getAppliedCredentials().getJwt(), "re-resolved from the holder on the TaskManager side");
    }
}
