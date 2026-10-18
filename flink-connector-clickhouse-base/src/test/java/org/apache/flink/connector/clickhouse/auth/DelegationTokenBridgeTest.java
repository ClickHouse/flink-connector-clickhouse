package org.apache.flink.connector.clickhouse.auth;

import org.apache.flink.connector.clickhouse.token.ClickHouseCredentials;
import org.apache.flink.connector.clickhouse.token.ClickHouseCredentialsHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class DelegationTokenBridgeTest {

    private static long inOneHour() {
        return System.currentTimeMillis() + 3_600_000L;
    }

    @AfterEach
    void reset() {
        ClickHouseCredentialsHolder.reset();
    }

    @Test
    void currentIsNullBeforeAnyToken() {
        assertNull(DelegationTokenBridge.current());
    }

    @Test
    void currentReturnsSameInstanceUntilHolderChanges() {
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("jwt-1", 10L));
        ResolvedCredentials first = DelegationTokenBridge.current();
        assertSame(first, DelegationTokenBridge.current());
        assertEquals("jwt-1", first.getJwt());
        assertEquals(10L, first.getValidUntilMillis());

        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("jwt-2", 20L));
        ResolvedCredentials second = DelegationTokenBridge.current();
        assertNotSame(first, second);
        assertEquals("jwt-2", second.getJwt());
    }

    @Test
    void awaitReturnsImmediatelyWhenPresent() throws Exception {
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("jwt-1", inOneHour()));
        assertEquals("jwt-1", DelegationTokenBridge.await(Duration.ofMillis(1)).getJwt());
    }

    @Test
    void awaitPicksUpTokenThatArrivesLater() throws Exception {
        Thread publisher = new Thread(() -> {
            try {
                Thread.sleep(300);
            } catch (InterruptedException ignored) {
            }
            ClickHouseCredentialsHolder.set(new ClickHouseCredentials("late", inOneHour()));
        });
        publisher.start();
        assertEquals("late", DelegationTokenBridge.await(Duration.ofSeconds(5)).getJwt());
        publisher.join();
    }

    @Test
    void awaitTimesOutWithDiagnosticMessage() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> DelegationTokenBridge.await(Duration.ofMillis(200)));
        assertTrue(e.getMessage().contains("security.delegation.token.provider.clickhouse.oauth.token-endpoint"), e.getMessage());
        assertTrue(e.getMessage().contains("lib/"), e.getMessage());
        assertTrue(e.getMessage().contains("classloader.parent-first-patterns.default"), e.getMessage());
    }

    @Test
    void awaitIgnoresAnExpiredToken() {
        long expiredAt = System.currentTimeMillis() - 1_000L;
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("stale", expiredAt));
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> DelegationTokenBridge.await(Duration.ofMillis(200)));
        assertTrue(e.getMessage().contains("expired at " + java.time.Instant.ofEpochMilli(expiredAt)), e.getMessage());
        assertTrue(e.getMessage().contains("Could not re-obtain the ClickHouse JWT"), e.getMessage());
    }

    @Test
    void awaitReturnsWhenAFreshTokenReplacesAnExpiredOne() throws Exception {
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("stale", System.currentTimeMillis() - 1_000L));
        Thread publisher = new Thread(() -> {
            try {
                Thread.sleep(300);
            } catch (InterruptedException ignored) {
            }
            ClickHouseCredentialsHolder.set(new ClickHouseCredentials("fresh", inOneHour()));
        });
        publisher.start();
        assertEquals("fresh", DelegationTokenBridge.await(Duration.ofSeconds(5)).getJwt());
        publisher.join();
    }

    @Test
    void reBroadcastOfTheSameJwtKeepsTheSameInstance() {
        long validUntil = inOneHour();
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("jwt-1", validUntil));
        ResolvedCredentials first = DelegationTokenBridge.current();
        // The provider re-sends its last good token while the identity provider is down: a new holder object.
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("jwt-1", validUntil));
        assertSame(first, DelegationTokenBridge.current());
    }

    @Test
    void tokenIsExpiredFromItsValidUntilInstant() {
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("jwt-1", 1_000L));
        ResolvedCredentials c = DelegationTokenBridge.current();
        assertFalse(c.isExpired(999L));
        assertTrue(c.isExpired(1_000L));
    }

    @Test
    void resolvedCredentialsToStringIsRedacted() {
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("abcdefghijkl", 10L));
        assertFalse(DelegationTokenBridge.current().toString().contains("abcdefghijkl"));
    }
}
