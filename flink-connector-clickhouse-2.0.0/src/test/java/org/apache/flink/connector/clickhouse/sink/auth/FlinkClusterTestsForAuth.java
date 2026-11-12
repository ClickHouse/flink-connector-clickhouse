package org.apache.flink.connector.clickhouse.sink.auth;

import com.clickhouse.data.ClickHouseFormat;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.connector.clickhouse.token.ClickHouseCredentials;
import org.apache.flink.connector.clickhouse.token.ClickHouseCredentialsHolder;
import org.apache.flink.connector.test.embedded.flink.EmbeddedFlinkClusterForTests;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterAll;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.clickhouse.sink.ClickHouseSinkTestUtils.*;

/**
 * Base for delegation-token tests: a MiniCluster started with the ClickHouse token-provider configuration.
 *
 * <p>Not an extension of {@code FlinkClusterTests}: Flink calls the token provider while the cluster starts, so the
 * provider configuration must be passed at startup, and these tests need no ClickHouse container.
 */
abstract class FlinkClusterTestsForAuth {

    protected static final String PROVIDER_PREFIX = "security.delegation.token.provider.clickhouse.";

    private static boolean clusterStarted;

    /** Environment variable, trimmed; {@code null} if unset or blank. */
    protected static String env(String name) {
        String v = System.getenv(name);
        return v == null || v.trim().isEmpty() ? null : v.trim();
    }

    protected static String env(String name, String defaultValue) {
        String v = env(name);
        return v != null ? v : defaultValue;
    }

    protected static void put(Configuration conf, String key, String value) {
        conf.set(ConfigOptions.key(key).stringType().noDefaultValue(), value);
    }

    /** Flink configuration for the provider: OAuth2 client credentials plus renewal settings. */
    protected static Configuration oauthProviderConfig(String tokenEndpoint, String clientId, String clientSecret,
                                                       String audience, String scope, String renewalRatio,
                                                       String retryBackoff) {
        Configuration conf = new Configuration();
        put(conf, PROVIDER_PREFIX + "oauth.token-endpoint", tokenEndpoint);
        put(conf, PROVIDER_PREFIX + "oauth.client-id", clientId);
        put(conf, PROVIDER_PREFIX + "oauth.client-secret", clientSecret);
        if (audience != null) {
            put(conf, PROVIDER_PREFIX + "oauth.audience", audience);
        }
        if (scope != null) {
            put(conf, PROVIDER_PREFIX + "oauth.scope", scope);
        }
        if (renewalRatio != null) {
            put(conf, "security.delegation.tokens.renewal.time-ratio", renewalRatio);
        }
        put(conf, "security.delegation.tokens.renewal.retry.backoff", retryBackoff);
        return conf;
    }

    /** Starts the MiniCluster; Flink obtains the first token during startup. */
    protected static void startCluster(Configuration conf) throws Exception {
        EmbeddedFlinkClusterForTests.setUp(conf);
        clusterStarted = true;
    }

    /** Runs after the subclass's own {@code @AfterAll}. */
    @AfterAll
    static void stopCluster() {
        // Only stop the cluster this class started: a skipped setUp must not stop another class's cluster twice.
        if (clusterStarted) {
            EmbeddedFlinkClusterForTests.tearDown();
            clusterStarted = false;
        }
        ClickHouseCredentialsHolder.reset();
    }

    /** The MiniCluster's environment (Flink 2.x test utilities). */
    protected static StreamExecutionEnvironment flinkEnv() {
        StreamExecutionEnvironment env = EmbeddedFlinkClusterForTests.getMiniCluster().getTestStreamEnvironment();
        env.setParallelism(1);
        return env;
    }

    /** A CSV sink in delegation-token mode with small batches, so every ~second is a fresh insert. */
    protected static ClickHouseAsyncSink<String> csvSink(ClickHouseClientConfig config, int maxBatchSize) {
        return ClickHouseAsyncSink.<String>builder()
                .setElementConverter(new ClickHouseConvertor<>(String.class))
                .setMaxBatchSize(maxBatchSize)
                .setMaxInFlightRequests(MAX_IN_FLIGHT_REQUESTS)
                .setMaxBufferedRequests(MAX_BUFFERED_REQUESTS)
                .setMaxBatchSizeInBytes(MAX_BATCH_SIZE_IN_BYTES)
                .setMaxTimeInBufferMS(1000)
                .setMaxRecordSizeInBytes(MAX_RECORD_SIZE_IN_BYTES)
                .setClickHouseClientConfig(config)
                .setClickHouseFormat(ClickHouseFormat.CSV)
                .build();
    }

    /** Records every distinct JWT the receiver publishes in this JVM while it runs. */
    protected static final class TokenWatcher implements AutoCloseable {
        private final Set<String> seen = ConcurrentHashMap.newKeySet();
        private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        TokenWatcher() {
            executor.scheduleAtFixedRate(() -> {
                ClickHouseCredentials c = ClickHouseCredentialsHolder.get();
                if (c != null) {
                    seen.add(c.getJwt());
                }
            }, 0, 500, TimeUnit.MILLISECONDS);
        }

        int distinctTokens() {
            return seen.size();
        }

        @Override
        public void close() {
            executor.shutdownNow();
        }
    }
}
