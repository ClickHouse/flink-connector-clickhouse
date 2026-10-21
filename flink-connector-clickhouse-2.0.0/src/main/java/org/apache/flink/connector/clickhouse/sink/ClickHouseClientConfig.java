package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.config.BatchFailureStrategy;
import com.clickhouse.config.RetryPolicy;
import org.apache.flink.connector.clickhouse.auth.AuthMode;
import org.apache.flink.connector.clickhouse.auth.DelegationTokenBridge;
import org.apache.flink.connector.clickhouse.auth.ResolvedCredentials;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Connection settings for the sink, serialized to the task managers. Constructing one never
 * touches the network: {@link ClickHouseAsyncSinkBuilder#build()} verifies connectivity on the
 * job driver via {@link #verifyConnectivity()}, while the Table API factory relies on its
 * planning-time DESCRIBE through {@link #createPlanningClient(Map)} failing instead.
 * Delegation-token configs ({@link #forDelegationToken}) skip the driver-side check: the job
 * driver never receives tokens, so the first connection happens on the task managers.
 */
public class ClickHouseClientConfig implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseClientConfig.class);
    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_MAX_RETRIES = 3;

    /** The Flink API that built this config, sent as the product name's {@code api:} tag. */
    public enum Api { DATASTREAM, TABLE }

    private final String url;
    private final String username;
    private final String password;
    private final String database;
    private final String tableName;
    private Boolean supportDefault = null;
    private final Map<String, String> options;
    private final Map<String, String> serverSettings;
    private boolean enableJsonSupportAsString;
    private transient Client client = null;
    private RetryPolicy retryPolicy = RetryPolicy.forever();
    private BatchFailureStrategy batchFailureStrategy = BatchFailureStrategy.STOP_FLINK;
    private Api api = Api.DATASTREAM;
    private AuthMode authMode = AuthMode.PASSWORD;
    private Duration delegationTokenWaitTimeout = Duration.ofSeconds(60);
    private transient ResolvedCredentials appliedCredentials = null;
    private transient ResolvedCredentials authFailureToken = null;
    private transient int authFailuresWithToken = 0;

    /** Consecutive auth failures (516/720) tolerated with the same JWT before the job fails. */
    public static final int MAX_AUTH_FAILURES_PER_TOKEN = 3;

    public ClickHouseClientConfig(String url, String username, String password, String database, String tableName, Map<String, String> options, Map<String, String> serverSettings, boolean enableJsonSupportAsString) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.database = database;
        this.tableName = tableName;
        this.options = new HashMap<>(Optional.ofNullable(options).orElseGet(HashMap::new));
        this.serverSettings = new HashMap<>(Optional.ofNullable(serverSettings).orElseGet(HashMap::new));
        this.enableJsonSupportAsString = enableJsonSupportAsString;
        LOG.info("ClickHouseClientConfig: url={}, user={}, password=******, database={}", url, username, database);
    }

    public ClickHouseClientConfig(String url, String username, String password, String database, String tableName) {
        this(url, username, password, database, tableName, new HashMap<>(), new HashMap<>(), false);
    }

    public ClickHouseClientConfig(String url, String username, String password, String database, String tableName, boolean enableJsonSupport) {
        this(url, username, password, database, tableName, new HashMap<>(), new HashMap<>(), enableJsonSupport);
    }

    private ClickHouseClientConfig(String url, String database, String tableName, AuthMode authMode) {
        this.url = url;
        this.username = null;
        this.password = null;
        this.database = database;
        this.tableName = tableName;
        this.options = new HashMap<>();
        this.serverSettings = new HashMap<>();
        this.enableJsonSupportAsString = false; // same default as the 5-arg password constructor
        this.authMode = authMode;
        LOG.info("ClickHouseClientConfig: url={}, auth=delegation-token (JWT), database={}", url, database);
    }

    /**
     * Config that authenticates to ClickHouse Cloud with short-lived JWTs from Flink's delegation
     * token framework. Requires flink-connector-clickhouse-token in Flink's lib/ and
     * {@code security.delegation.token.provider.clickhouse.oauth.*} in the Flink configuration.
     */
    public static ClickHouseClientConfig forDelegationToken(String url, String database, String tableName) {
        requireHttpsOrLoopback(url);
        return new ClickHouseClientConfig(url, database, tableName, AuthMode.DELEGATION_TOKEN);
    }

    /** ClickHouse Cloud: "Always send JWTs over HTTPS". Plain HTTP is allowed only for loopback test endpoints. */
    static void requireHttpsOrLoopback(String url) {
        URI uri = URI.create(url);
        String scheme = uri.getScheme();
        String host = uri.getHost();
        boolean loopback = "localhost".equalsIgnoreCase(host) || "127.0.0.1".equals(host)
                || "[::1]".equals(host) || "::1".equals(host);
        if ("https".equalsIgnoreCase(scheme) || ("http".equalsIgnoreCase(scheme) && loopback)) {
            return;
        }
        throw new IllegalArgumentException("ClickHouse JWTs must be sent over HTTPS (got " + url
                + "); plain http:// is only allowed for loopback test endpoints");
    }

    /** Deep copy for DynamicTableSink#copy(); the cached client is not shared. */
    public ClickHouseClientConfig copy() {
        ClickHouseClientConfig copy = authMode == AuthMode.DELEGATION_TOKEN
                ? new ClickHouseClientConfig(url, database, tableName, AuthMode.DELEGATION_TOKEN)
                : new ClickHouseClientConfig(url, username, password, database, tableName, options, serverSettings,
                        enableJsonSupportAsString);
        if (authMode == AuthMode.DELEGATION_TOKEN) {
            copy.setOptions(options);
            copy.setServerSettings(serverSettings);
            copy.setEnableJsonSupportAsString(enableJsonSupportAsString);
        }
        copy.setDelegationTokenWaitTimeout(delegationTokenWaitTimeout);
        copy.setSupportDefault(supportDefault);
        copy.setRetryPolicy(retryPolicy);
        copy.setBatchFailureStrategy(batchFailureStrategy);
        copy.setApi(api);
        return copy;
    }

    /**
     * Pings up to {@link #DEFAULT_MAX_RETRIES} times, 1s apart, on a probe client closed either
     * way — a fixed bound, independent of the retry policy, which governs batch retries. An
     * interrupt during the retry sleep is re-asserted and fails with its own message; one that
     * lands inside client-v2's {@code ping()} is swallowed there (it returns false with the flag
     * cleared) and counts as a failed attempt.
     */
    public void verifyConnectivity() {
        if (authMode == AuthMode.DELEGATION_TOKEN) {
            LOG.info("Skipping the connectivity check for delegation-token auth: the job driver has no token; "
                    + "the first connection happens on the task managers");
            return;
        }
        try (Client probe = initClient(database, Map.of())) {
            boolean isServerAlive = false;
            for (int i = 0; i < DEFAULT_MAX_RETRIES && !isServerAlive; i++) {
                isServerAlive = probe.ping();
                if (!isServerAlive && i < DEFAULT_MAX_RETRIES - 1) {
                    LOG.warn("Ping failed; will retry up to {} times in {} seconds.", DEFAULT_MAX_RETRIES, 1);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while checking ClickHouse connectivity.", e);
                    }
                }
            }
            if (!isServerAlive) {
                throw new RuntimeException("ClickHouse server is not accessible. Please check your configuration or ClickHouse server.");
            }
        }
    }

    /**
     * A fresh, uncached client for planning; the caller closes it. The extra server settings
     * reach this client only, never the serialized runtime config.
     */
    public Client createPlanningClient(Map<String, String> planningServerSettings) {
        if (authMode == AuthMode.DELEGATION_TOKEN) {
            throw new UnsupportedOperationException("Table API planning does not support delegation-token auth: "
                    + "the planner has no ClickHouse JWT. Use username/password for Table API sinks.");
        }
        return initClient(database, planningServerSettings);
    }

    private Client initClient(String database, Map<String, String> extraServerSettings) {
        Client.Builder clientBuilder = new Client.Builder().addEndpoint(url);
        if (authMode != AuthMode.DELEGATION_TOKEN) {
            // Before setOptions, as always: auth keys in options still override these.
            clientBuilder.setUsername(username).setPassword(password);
        }
        clientBuilder
                .setDefaultDatabase(database)
                .setClientName(productName())
                .setOption(ClientConfigProperties.ASYNC_OPERATIONS.getKey(), "true")
                .setOptions(options);
        if (authMode == AuthMode.DELEGATION_TOKEN) {
            clientBuilder.useBearerTokenAuth(appliedCredentials.getJwt());
        }

        for (Map.Entry<String, String> entry : serverSettings.entrySet()) {
            clientBuilder.serverSetting(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, String> entry : extraServerSettings.entrySet()) {
            clientBuilder.serverSetting(entry.getKey(), entry.getValue());
        }
        return clientBuilder.build();
    }

    private String productName() {
        return String.format("Flink-ClickHouse-Sink/%s (fv:flink/%s, lv:scala/%s, api:%s)", ClickHouseSinkVersion.getVersion(),
                EnvironmentInformation.getVersion(), EnvironmentInformation.getScalaVersion(), api.name().toLowerCase(Locale.ROOT));
    }

    public Client createClient(String database) {
        if (this.client == null) {
            if (authMode == AuthMode.DELEGATION_TOKEN) {
                try {
                    appliedCredentials = DelegationTokenBridge.await(delegationTokenWaitTimeout);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for ClickHouse delegation token", e);
                }
            }
            this.client = initClient(database, Map.of());
        }
        return client;
    }

    /**
     * Applies a newer delegation token JWT to the live client, if one arrived since the last call.
     * Cheap no-op otherwise (one volatile read). Call before every insert.
     */
    public void refreshCredentialsIfChanged() {
        if (authMode != AuthMode.DELEGATION_TOKEN || client == null) {
            return;
        }
        ResolvedCredentials current = DelegationTokenBridge.current();
        if (current == null || current == appliedCredentials) {
            return;
        }
        client.updateBearerToken(current.getJwt());
        appliedCredentials = current;
        LOG.info("Applied re-obtained ClickHouse delegation token {}", current);
    }

    public boolean isDelegationTokenMode() {
        return authMode == AuthMode.DELEGATION_TOKEN;
    }

    public void setDelegationTokenWaitTimeout(Duration delegationTokenWaitTimeout) {
        this.delegationTokenWaitTimeout = Objects.requireNonNull(delegationTokenWaitTimeout, "delegationTokenWaitTimeout");
    }

    ResolvedCredentials getAppliedCredentials() {
        return appliedCredentials;
    }

    /**
     * Called when ClickHouse rejected the JWT a request was sent with (516/720). Returns {@code true}
     * if the batch should be retried: the token had expired (renewal is late), a newer token arrived
     * since the request was sent (expiry race), or this token has failed fewer than
     * {@link #MAX_AUTH_FAILURES_PER_TOKEN} times in a row.
     * {@code false} means the token is rejected permanently (e.g. wrong audience in the Cloud JWT
     * provider), so the job should fail with a clear error instead of retrying forever.
     */
    public synchronized boolean onAuthFailure(ResolvedCredentials usedForRequest) {
        if (usedForRequest != null && usedForRequest.isExpired(System.currentTimeMillis())) {
            // Rejected because it expired (renewal is late, e.g. identity provider down), not misconfigured.
            return true;
        }
        ResolvedCredentials latest = authMode == AuthMode.DELEGATION_TOKEN ? DelegationTokenBridge.current() : null;
        if (latest != null && latest != usedForRequest) {
            authFailureToken = latest;
            authFailuresWithToken = 0;
            return true;
        }
        if (usedForRequest != authFailureToken) {
            authFailureToken = usedForRequest;
            authFailuresWithToken = 0;
        }
        authFailuresWithToken++;
        return authFailuresWithToken <= MAX_AUTH_FAILURES_PER_TOKEN;
    }

    /** Resets the auth-failure budget after a successful insert. */
    public synchronized void onWriteSuccess() {
        authFailureToken = null;
        authFailuresWithToken = 0;
    }

    public Client createClient() {
        return createClient(this.database);
    }

    public String getTableName() {
        return tableName;
    }

    public void setSupportDefault(Boolean supportDefault) {
        this.supportDefault = supportDefault;
    }

    public Boolean getSupportDefault() {
        return supportDefault;
    }

    public void setOptions(Map<String, String> options) {
        if (options != null) {
            this.options.putAll(options);
        }
    }

    public void setServerSettings(Map<String, String> serverSettings) {
        if (serverSettings != null) {
            this.serverSettings.putAll(serverSettings);
        }
    }

    public RetryPolicy getRetryPolicy() { return retryPolicy; }

    public void setRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
    }

    public BatchFailureStrategy getBatchFailureStrategy() { return batchFailureStrategy; }

    public void setBatchFailureStrategy(BatchFailureStrategy batchFailureStrategy) {
        this.batchFailureStrategy = Objects.requireNonNull(
                batchFailureStrategy,"batchFailureStrategy must not be null");
    }

    public void setEnableJsonSupportAsString(boolean enableJsonSupportAsString) {
        this.enableJsonSupportAsString = enableJsonSupportAsString;
    }

    public Boolean getEnableJsonSupportAsString() { return  enableJsonSupportAsString; }

    public Api getApi() { return api; }

    public void setApi(Api api) {
        this.api = Objects.requireNonNull(api, "api must not be null");
    }

}
