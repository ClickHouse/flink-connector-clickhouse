package org.apache.flink.connector.clickhouse.sink.auth;

import com.clickhouse.client.api.Client;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.connector.clickhouse.token.ClickHouseCredentials;
import org.apache.flink.connector.clickhouse.token.ClickHouseCredentialsHolder;
import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * No fakes: the provider requests tokens from a real identity provider (OAuth2 client credentials) and the
 * sink writes to a real ClickHouse Cloud service that validates them.
 *
 * <p>Runs in Cloud mode only ({@code CLICKHOUSE_VERSION=cloud}, {@code CLICKHOUSE_CLOUD_HOST},
 * {@code CLICKHOUSE_CLOUD_PASSWORD}) and when the identity provider is configured
 * ({@code CLICKHOUSE_CLOUD_OAUTH_TOKEN_ENDPOINT}, {@code _CLIENT_ID}, {@code _CLIENT_SECRET}, optional
 * {@code _AUDIENCE} / {@code _SCOPE}). Each run writes, through the identity provider's JWT, to a table in the
 * per-run database {@code flink_connector_test_<timestamp>} that the admin login creates and drops. The
 * service's JWT provider must therefore grant the token INSERT and SELECT on {@code flink_connector_test_*.*}.
 */
class ClickHouseDelegationTokenCloudRealIdpTests extends FlinkClusterTestsForAuth {

    private static final int ROWS = 400;
    private static final long TARGET_RENEWAL_SECONDS = 40;

    private static String url;
    private static String database;
    private static String table;
    private static boolean databaseCreated;

    @BeforeAll
    static void setUp() throws Exception {
        assumeTrue(ClickHouseServerForTests.isCloud(),
                "needs CLICKHOUSE_VERSION=cloud (a ClickHouse Cloud service with a JWT provider)");
        for (String name : new String[] {"CLICKHOUSE_CLOUD_OAUTH_TOKEN_ENDPOINT", "CLICKHOUSE_CLOUD_OAUTH_CLIENT_ID",
                "CLICKHOUSE_CLOUD_OAUTH_CLIENT_SECRET"}) {
            assumeTrue(env(name) != null, name + " not set; skipping real-identity-provider Cloud test");
        }
        ClickHouseServerForTests.setUp(); // CLICKHOUSE_CLOUD_HOST/PASSWORD; creates flink_connector_test_<ts>
        databaseCreated = true;
        url = ClickHouseServerForTests.getURL();
        database = ClickHouseServerForTests.getDatabase();
        table = "flink_dt";
        ClickHouseServerForTests.executeSql("CREATE TABLE `" + database + "`.`" + table
                + "` (id UInt64, payload String) ENGINE = MergeTree ORDER BY id");

        // Renew about every 40s whatever lifetime the identity provider issues (24h tokens need a tiny
        // ratio to rotate during the run; 60s tokens must not get one, or Flink re-requests in a loop).
        String ratio = env("CLICKHOUSE_CLOUD_OAUTH_RENEWAL_RATIO",
                String.valueOf(Math.min(0.75, TARGET_RENEWAL_SECONDS / (double) probeTokenLifetimeSeconds())));
        startCluster(oauthProviderConfig(env("CLICKHOUSE_CLOUD_OAUTH_TOKEN_ENDPOINT"), env("CLICKHOUSE_CLOUD_OAUTH_CLIENT_ID"),
                env("CLICKHOUSE_CLOUD_OAUTH_CLIENT_SECRET"), env("CLICKHOUSE_CLOUD_OAUTH_AUDIENCE"),
                env("CLICKHOUSE_CLOUD_OAUTH_SCOPE"), ratio, "5s"));
    }

    @AfterAll
    static void dropDatabase() throws Exception {
        if (databaseCreated) {
            ClickHouseServerForTests.executeSql("DROP DATABASE IF EXISTS `" + database + "`");
            databaseCreated = false;
        }
    }

    /** Requests one token up front to learn the identity provider's lifetime ({@code expires_in}). */
    private static long probeTokenLifetimeSeconds() throws Exception {
        StringBuilder form = new StringBuilder("grant_type=client_credentials");
        for (String[] kv : new String[][] {{"client_id", "CLICKHOUSE_CLOUD_OAUTH_CLIENT_ID"},
                {"client_secret", "CLICKHOUSE_CLOUD_OAUTH_CLIENT_SECRET"}, {"audience", "CLICKHOUSE_CLOUD_OAUTH_AUDIENCE"},
                {"scope", "CLICKHOUSE_CLOUD_OAUTH_SCOPE"}}) {
            if (env(kv[1]) != null) {
                form.append('&').append(kv[0]).append('=').append(URLEncoder.encode(env(kv[1]), StandardCharsets.UTF_8));
            }
        }
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(env("CLICKHOUSE_CLOUD_OAUTH_TOKEN_ENDPOINT")))
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .POST(HttpRequest.BodyPublishers.ofString(form.toString())).build(),
                HttpResponse.BodyHandlers.ofString());
        Matcher m = Pattern.compile("\"expires_in\"\\s*:\\s*\"?(\\d+)").matcher(response.body());
        assertTrue(response.statusCode() == 200 && m.find(), "probe token request failed: HTTP " + response.statusCode());
        long lifetime = Long.parseLong(m.group(1));
        System.out.println("[oauth-cloud-test] identity provider token lifetime " + lifetime + "s");
        return lifetime;
    }

    /** Queries with the JWT the receiver published in this JVM, i.e. the same identity the sink uses. */
    private static long countRows() throws Exception {
        ClickHouseCredentials current = ClickHouseCredentialsHolder.get();
        assertNotNull(current, "the receiver must have published a token");
        try (Client client = new Client.Builder().addEndpoint(url).useBearerTokenAuth(current.getJwt()).build()) {
            return client.queryAll("SELECT count() FROM `" + database + "`.`" + table + "`").get(0).getLong(1);
        }
    }

    @Test
    void sinkWritesThroughRealIdentityProviderTokensAcrossRotations() throws Exception {
        StreamExecutionEnvironment env = flinkEnv();
        // ~120s of writes so several re-obtains happen while the sink is running.
        env.fromSequence(1, ROWS)
                .map(i -> {
                    Thread.sleep(300);
                    return i + ",row-" + i;
                })
                .returns(Types.STRING)
                .sinkTo(csvSink(ClickHouseClientConfig.forDelegationToken(url, database, table), 20));

        int distinctTokens;
        try (TokenWatcher watcher = new TokenWatcher()) {
            JobClient job = env.executeAsync("delegation-token-oauth-real-idp");
            job.getJobExecutionResult().get(300, TimeUnit.SECONDS);
            distinctTokens = watcher.distinctTokens();
        }

        assertEquals(ROWS, countRows(), "rows written to " + database + "." + table);
        assertTrue(distinctTokens >= 2, "expected at least one rotation, saw JWTs: " + distinctTokens);
        System.out.println("[oauth-cloud-test] " + database + "." + table + ": " + ROWS + " rows, distinct JWTs " + distinctTokens);
    }
}
