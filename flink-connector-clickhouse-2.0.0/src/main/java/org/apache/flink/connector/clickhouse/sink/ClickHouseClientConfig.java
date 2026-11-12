package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.config.BatchFailureStrategy;
import com.clickhouse.config.RetryPolicy;
import org.apache.flink.connector.clickhouse.auth.AuthMode;
import org.apache.flink.connector.clickhouse.auth.DelegationTokenAuth;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
    private transient DelegationTokenAuth delegationTokenAuth = null;

    /** client-v2 options that set credentials, rejected in delegation-token mode. */
    private static final List<String> AUTH_OPTION_KEYS = Arrays.asList(
            ClientConfigProperties.USER.getKey(), ClientConfigProperties.PASSWORD.getKey(),
            ClientConfigProperties.ACCESS_TOKEN.getKey(), ClientConfigProperties.BEARERTOKEN_AUTH.getKey(),
            ClientConfigProperties.SSL_AUTH.getKey());
    private static final String AUTHORIZATION_HEADER_OPTION = ClientConfigProperties.httpHeader("Authorization");

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
            clientBuilder.setUsername(username).setPassword(password);
        }
        clientBuilder
                .setDefaultDatabase(database)
                .setClientName(productName())
                .setOption(ClientConfigProperties.ASYNC_OPERATIONS.getKey(), "true")
                .setOptions(options);
        if (authMode == AuthMode.DELEGATION_TOKEN) {
            clientBuilder.useBearerTokenAuth(delegationTokenAuth.getAppliedCredentials().getJwt());
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
                DelegationTokenAuth auth = new DelegationTokenAuth(delegationTokenWaitTimeout);
                auth.awaitFirst();
                delegationTokenAuth = auth;
            }
            this.client = initClient(database, Map.of());
        }
        return client;
    }

    /** Delegation-token state of the client from {@link #createClient()}; {@code null} in password mode or before. */
    public DelegationTokenAuth getDelegationTokenAuth() {
        return delegationTokenAuth;
    }

    public boolean isDelegationTokenMode() {
        return authMode == AuthMode.DELEGATION_TOKEN;
    }

    public void setDelegationTokenWaitTimeout(Duration delegationTokenWaitTimeout) {
        this.delegationTokenWaitTimeout = Objects.requireNonNull(delegationTokenWaitTimeout, "delegationTokenWaitTimeout");
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
            if (authMode == AuthMode.DELEGATION_TOKEN) {
                rejectAuthOptions(options);
            }
            this.options.putAll(options);
        }
    }

    private static void rejectAuthOptions(Map<String, String> options) {
        for (String key : options.keySet()) {
            if (AUTH_OPTION_KEYS.contains(key) || AUTHORIZATION_HEADER_OPTION.equalsIgnoreCase(key)) {
                throw new IllegalArgumentException("Option '" + key + "' sets ClickHouse credentials, which "
                        + "delegation-token auth does not allow: the JWT from Flink's delegation token framework is "
                        + "the only credential. Remove it from the options.");
            }
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
