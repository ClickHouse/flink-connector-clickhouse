package com.clickhouse.flink.integration;

import com.clickhouse.flink.Cluster;
import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.io.File;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Delegation tokens on a real Flink cluster: Docker JobManager + 2 TaskManagers, the token jar in
 * lib/, a job jar that contains only the sink, a real OAuth identity provider and ClickHouse Cloud.
 * Proves what the MiniCluster tests cannot: ServiceLoader discovery from lib/, parent-first
 * classloading across the lib/ and user-code classloaders, and RPC delivery of the JWT to
 * TaskManagers running in separate JVMs.
 *
 * <p>Runs in Cloud mode only ({@code CLICKHOUSE_VERSION=cloud}, {@code CLICKHOUSE_CLOUD_HOST},
 * {@code CLICKHOUSE_CLOUD_PASSWORD}) and when the identity provider is configured
 * ({@code CLICKHOUSE_CLOUD_OAUTH_TOKEN_ENDPOINT}, {@code _CLIENT_ID}, {@code _CLIENT_SECRET}, optional
 * {@code _AUDIENCE} / {@code _SCOPE}). The service's JWT provider must grant the token INSERT and
 * SELECT on {@code flink_connector_test_*.*}: rows go to the per-run database.
 */
class DelegationTokenClusterTests {

    private static final String PROVIDER = "security.delegation.token.provider.clickhouse.";
    private static final String TABLE = "flink_dt_cluster";
    private static final int TASK_MANAGERS = 2;
    /** 2 subtasks at 1 row/s each: ~2 minutes, several ~40s token renewals. */
    private static final long ROWS = 240;
    private static final long TARGET_RENEWAL_SECONDS = 40;

    private static String flinkVersion;
    private static boolean databaseCreated;

    @BeforeAll
    static void setUp() throws Exception {
        assumeTrue(ClickHouseServerForTests.isCloud(),
                "needs CLICKHOUSE_VERSION=cloud (a ClickHouse Cloud service with a JWT provider)");
        for (String name : new String[] {"CLICKHOUSE_CLOUD_OAUTH_TOKEN_ENDPOINT", "CLICKHOUSE_CLOUD_OAUTH_CLIENT_ID",
                "CLICKHOUSE_CLOUD_OAUTH_CLIENT_SECRET"}) {
            assumeTrue(env(name).isPresent(), name + " not set; skipping real-cluster delegation-token test");
        }
        assumeTrue(!"scala".equalsIgnoreCase(System.getProperty("example.lang", "java")), "Java example only");
        flinkVersion = env("FLINK_VERSION").orElse("latest");

        ClickHouseServerForTests.setUp(false); // CLICKHOUSE_CLOUD_HOST/PASSWORD; creates flink_connector_test_<ts>
        databaseCreated = true;
        ClickHouseServerForTests.executeSql("CREATE TABLE `" + ClickHouseServerForTests.getDatabase() + "`.`" + TABLE
                + "` (id UInt64, payload String) ENGINE = MergeTree ORDER BY id");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (databaseCreated) {
            ClickHouseServerForTests.executeSql("DROP DATABASE IF EXISTS `" + ClickHouseServerForTests.getDatabase() + "`");
            databaseCreated = false;
        }
        ClickHouseServerForTests.tearDown();
    }

    @Test
    void jobWritesWithDelegationTokensAndEveryTaskManagerReceivesThem() throws Exception {
        String tokenJar = System.getProperty("clickhouse.token.jar", "");
        assertTrue(new File(tokenJar).isFile(), "token jar not built: '" + tokenJar + "'");
        String exampleJar = exampleJar();
        assertTrue(new File(exampleJar).isFile(),
                "example not built: " + exampleJar + " (run mvn clean package in its directory)");

        // Renew about every 40s whatever lifetime the identity provider issues.
        double ratio = Math.min(0.75, TARGET_RENEWAL_SECONDS / (double) probeTokenLifetimeSeconds());
        Cluster.Builder builder = new Cluster.Builder()
                .withFlinkVersion(flinkVersion)
                .withTaskManagers(TASK_MANAGERS)
                .withNetwork(ClickHouseServerForTests.getNetwork())
                .withLibJar(tokenJar)
                .withFlinkProperty(PROVIDER + "oauth.token-endpoint", env("CLICKHOUSE_CLOUD_OAUTH_TOKEN_ENDPOINT").get())
                .withFlinkProperty(PROVIDER + "oauth.client-id", env("CLICKHOUSE_CLOUD_OAUTH_CLIENT_ID").get())
                .withFlinkProperty(PROVIDER + "oauth.client-secret", env("CLICKHOUSE_CLOUD_OAUTH_CLIENT_SECRET").get())
                .withFlinkProperty("security.delegation.tokens.renewal.time-ratio", String.valueOf(ratio));
        env("CLICKHOUSE_CLOUD_OAUTH_AUDIENCE").ifPresent(v -> builder.withFlinkProperty(PROVIDER + "oauth.audience", v));
        env("CLICKHOUSE_CLOUD_OAUTH_SCOPE").ifPresent(v -> builder.withFlinkProperty(PROVIDER + "oauth.scope", v));

        Cluster cluster = builder.build();
        try {
            cluster.uploadJar(exampleJar);
            String jobId = cluster.runJob(cluster.listAllJars().get(0),
                    "com.clickhouse.example.delegationtoken.DelegationTokenJob", 2,
                    "-url", ClickHouseServerForTests.getURL(),
                    "-database", ClickHouseServerForTests.getDatabase(),
                    "-table", TABLE,
                    "-rows", String.valueOf(ROWS),
                    "-intervalMs", "1000");

            String state = awaitTerminalState(cluster, jobId, Duration.ofMinutes(6));
            String jobManagerLog = cluster.getContainerJobManager().getLogs();
            assertEquals("FINISHED", state, "job did not finish; JobManager log tail:\n" + tail(jobManagerLog));

            // 1. Every row arrived, written with delegation-token JWTs only (the job has no password).
            assertEquals(ROWS, ClickHouseServerForTests.countRows(TABLE));
            // 2. The JobManager obtained at least two JWTs: the job outlived a rotation.
            assertTrue(occurrences(jobManagerLog, "Obtained ClickHouse JWT") >= 2,
                    "expected a token rotation; JobManager log tail:\n" + tail(jobManagerLog));
            // 3. Every TaskManager JVM received the JWT over RPC, which a MiniCluster cannot show.
            for (GenericContainer<?> taskManager : cluster.getContainerTaskManagerList()) {
                String log = taskManager.getLogs();
                assertTrue(log.contains("Received ClickHouse JWT"), "TaskManager got no token:\n" + tail(log));
                assertFalse(log.contains("No ClickHouse delegation token received"), tail(log));
            }
        } finally {
            cluster.tearDown();
        }
    }

    private static String exampleJar() {
        String root = env("PWD").or(() -> env("GITHUB_WORKSPACE"))
                .orElseThrow(() -> new IllegalStateException("Can not get root path"));
        String folder = flinkVersion.equalsIgnoreCase("latest") || flinkVersion.startsWith("2.") ? "flink-v2" : "flink-v1.7";
        return String.format("%s/examples/maven/%s/delegation-token/target/delegation-token-1.0-SNAPSHOT.jar", root, folder);
    }

    private static String awaitTerminalState(Cluster cluster, String jobId, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        String state = cluster.jobStatus(jobId);
        while (!state.equals("FINISHED") && !state.equals("FAILED") && !state.equals("CANCELED")
                && System.nanoTime() < deadline) {
            Thread.sleep(2000);
            state = cluster.jobStatus(jobId);
        }
        return state;
    }

    /** One client-credentials request up front to learn the identity provider's token lifetime. */
    private static long probeTokenLifetimeSeconds() throws Exception {
        StringBuilder form = new StringBuilder("grant_type=client_credentials");
        for (String[] kv : new String[][] {{"client_id", "CLICKHOUSE_CLOUD_OAUTH_CLIENT_ID"},
                {"client_secret", "CLICKHOUSE_CLOUD_OAUTH_CLIENT_SECRET"}, {"audience", "CLICKHOUSE_CLOUD_OAUTH_AUDIENCE"},
                {"scope", "CLICKHOUSE_CLOUD_OAUTH_SCOPE"}}) {
            Optional<String> value = env(kv[1]);
            if (value.isPresent()) {
                form.append('&').append(kv[0]).append('=').append(URLEncoder.encode(value.get(), StandardCharsets.UTF_8));
            }
        }
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(env("CLICKHOUSE_CLOUD_OAUTH_TOKEN_ENDPOINT").get()))
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .POST(HttpRequest.BodyPublishers.ofString(form.toString())).build(),
                HttpResponse.BodyHandlers.ofString());
        Matcher m = Pattern.compile("\"expires_in\"\\s*:\\s*\"?(\\d+)").matcher(response.body());
        assertTrue(response.statusCode() == 200 && m.find(), "probe token request failed: HTTP " + response.statusCode());
        return Long.parseLong(m.group(1));
    }

    private static int occurrences(String text, String needle) {
        int count = 0;
        for (int i = text.indexOf(needle); i >= 0; i = text.indexOf(needle, i + needle.length())) {
            count++;
        }
        return count;
    }

    private static String tail(String log) {
        return log.length() <= 4096 ? log : "…" + log.substring(log.length() - 4096);
    }

    private static Optional<String> env(String name) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? Optional.empty() : Optional.of(value);
    }
}
