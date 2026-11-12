package org.apache.flink.connector.clickhouse.auth;

import com.clickhouse.client.api.Client;
import org.apache.flink.connector.clickhouse.token.ClickHouseCredentials;
import org.apache.flink.connector.clickhouse.token.ClickHouseCredentialsHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.clickhouse.auth.DelegationTokenAuth.MAX_AUTH_FAILURES_PER_TOKEN;
import static org.junit.jupiter.api.Assertions.*;

/** client-v2 builds clients lazily: no ClickHouse server needed, no request is sent. */
class DelegationTokenAuthTest {

    private static final Duration WAIT = Duration.ofSeconds(5);
    private Client client;

    @BeforeEach
    void buildClient() {
        client = new Client.Builder().addEndpoint("https://127.0.0.1:1/").useBearerTokenAuth("eyJ.initial.sig").build();
    }

    @AfterEach
    void reset() {
        client.close();
        ClickHouseCredentialsHolder.reset();
    }

    private static void publish(String jwt) {
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials(jwt, System.currentTimeMillis() + 60_000));
    }

    /** Counts every rejection, as if they arrived seconds apart. */
    private static DelegationTokenAuth withoutBackoff() {
        DelegationTokenAuth auth = new DelegationTokenAuth(WAIT, Duration.ZERO);
        auth.awaitFirst();
        return auth;
    }

    @Test
    void refreshSwapsTheJwt() {
        publish("eyJ.jwt1.sig");
        DelegationTokenAuth auth = new DelegationTokenAuth(WAIT);
        auth.awaitFirst();

        auth.refresh(client);
        assertEquals("eyJ.jwt1.sig", auth.getAppliedCredentials().getJwt(), "no change, no swap");

        publish("eyJ.jwt2.sig");
        auth.refresh(client);
        assertEquals("eyJ.jwt2.sig", auth.getAppliedCredentials().getJwt());
    }

    @Test
    void sameTokenRejectedRepeatedlyExhaustsTheRetryBudget() {
        publish("eyJ.jwt1.sig");
        DelegationTokenAuth auth = withoutBackoff();
        for (int i = 0; i < MAX_AUTH_FAILURES_PER_TOKEN; i++) {
            assertTrue(auth.onAuthFailure(auth.getAppliedCredentials()), "retry " + i);
        }
        assertFalse(auth.onAuthFailure(auth.getAppliedCredentials()), "budget exhausted for the same token");
    }

    @Test
    void newerTokenAllowsRetryAndResetsTheBudget() {
        publish("eyJ.jwt1.sig");
        DelegationTokenAuth auth = withoutBackoff();
        ResolvedCredentials used = auth.getAppliedCredentials();
        for (int i = 0; i < MAX_AUTH_FAILURES_PER_TOKEN; i++) {
            auth.onAuthFailure(used);
        }
        publish("eyJ.jwt2.sig"); // a newer token arrived: the failed request used a stale one
        assertTrue(auth.onAuthFailure(used));
        for (int i = 0; i < MAX_AUTH_FAILURES_PER_TOKEN; i++) {
            auth.refresh(client);
            assertTrue(auth.onAuthFailure(auth.getAppliedCredentials()), "fresh budget for the new token, retry " + i);
        }
    }

    @Test
    void successfulWriteResetsTheBudget() {
        publish("eyJ.jwt1.sig");
        DelegationTokenAuth auth = withoutBackoff();
        for (int i = 0; i < MAX_AUTH_FAILURES_PER_TOKEN; i++) {
            auth.onAuthFailure(auth.getAppliedCredentials());
        }
        auth.onWriteSuccess();
        assertTrue(auth.onAuthFailure(auth.getAppliedCredentials()));
    }

    @Test
    void expiredTokenRejectionsNeverExhaustTheBudget() throws Exception {
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("eyJ.short.sig", System.currentTimeMillis() + 300));
        DelegationTokenAuth auth = withoutBackoff();
        ResolvedCredentials used = auth.getAppliedCredentials();
        Thread.sleep(400); // the token expires while renewal is late (identity provider down)
        for (int i = 0; i < 10; i++) {
            assertTrue(auth.onAuthFailure(used), "rejection of an expired token is never permanent, attempt " + i);
        }
    }

    @Test
    void reBroadcastOfTheSameJwtDoesNotResetTheBudget() {
        long validUntil = System.currentTimeMillis() + 60_000;
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("eyJ.jwt1.sig", validUntil));
        DelegationTokenAuth auth = withoutBackoff();
        ResolvedCredentials used = auth.getAppliedCredentials();
        for (int i = 0; i < MAX_AUTH_FAILURES_PER_TOKEN; i++) {
            assertTrue(auth.onAuthFailure(used));
        }
        // The JobManager re-sends its last good token after an identity provider failure: not a new token.
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("eyJ.jwt1.sig", validUntil));
        auth.refresh(client);
        assertSame(used, auth.getAppliedCredentials());
        assertFalse(auth.onAuthFailure(used), "budget still exhausted: the same JWT keeps being rejected");
    }

    @Test
    void rejectionsOfTheSameTokenWithinTheBackoffCountOnce() {
        publish("eyJ.jwt1.sig");
        DelegationTokenAuth auth = new DelegationTokenAuth(WAIT);
        auth.awaitFirst();
        // Concurrent in-flight requests rejected together, or a sub-second skew between validUntil and the JWT exp.
        for (int i = 0; i < 10 * MAX_AUTH_FAILURES_PER_TOKEN; i++) {
            assertTrue(auth.onAuthFailure(auth.getAppliedCredentials()), "rejection " + i);
        }
    }

    @Test
    void awaitUsableBacksOffBeforeResendingARejectedToken() {
        publish("eyJ.jwt1.sig");
        DelegationTokenAuth auth = new DelegationTokenAuth(WAIT);
        auth.awaitFirst();
        ResolvedCredentials used = auth.awaitUsable(client);
        assertTrue(auth.onAuthFailure(used));

        long start = System.nanoTime();
        assertSame(used, auth.awaitUsable(client), "no newer token: the same one is re-sent");
        assertTrue(Duration.ofNanos(System.nanoTime() - start).toMillis() >= 900, "re-sent without backing off");
    }

    @Test
    void awaitUsableStopsBackingOffWhenANewerTokenArrives() {
        publish("eyJ.jwt1.sig");
        DelegationTokenAuth auth = new DelegationTokenAuth(WAIT, Duration.ofSeconds(30));
        auth.awaitFirst();
        assertTrue(auth.onAuthFailure(auth.awaitUsable(client)));

        CompletableFuture.runAsync(() -> publish("eyJ.jwt2.sig"), CompletableFuture.delayedExecutor(200, TimeUnit.MILLISECONDS));
        long start = System.nanoTime();
        assertEquals("eyJ.jwt2.sig", auth.awaitUsable(client).getJwt());
        assertTrue(Duration.ofNanos(System.nanoTime() - start).toMillis() < 5000, "waited for the full backoff");
    }

    @Test
    void awaitUsableNeverSendsAnExpiredToken() throws Exception {
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("eyJ.short.sig", System.currentTimeMillis() + 300));
        DelegationTokenAuth auth = new DelegationTokenAuth(WAIT);
        auth.awaitFirst();
        Thread.sleep(400); // expired while renewal is late (identity provider down)

        CompletableFuture.runAsync(() -> publish("eyJ.fresh.sig"), CompletableFuture.delayedExecutor(200, TimeUnit.MILLISECONDS));
        assertEquals("eyJ.fresh.sig", auth.awaitUsable(client).getJwt(), "waits for the renewed token");
    }

    @Test
    void awaitUsableFailsWithTheExpiryMessageWhenRenewalNeverComes() throws Exception {
        ClickHouseCredentialsHolder.set(new ClickHouseCredentials("eyJ.short.sig", System.currentTimeMillis() + 300));
        DelegationTokenAuth auth = new DelegationTokenAuth(Duration.ofMillis(200));
        auth.awaitFirst();
        Thread.sleep(400);

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> auth.awaitUsable(client));
        assertTrue(e.getMessage().contains("expired at"), e.getMessage());
    }
}
