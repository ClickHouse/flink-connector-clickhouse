package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.config.BatchFailureStrategy;
import com.clickhouse.config.RetryPolicy;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Connection settings for the sink, serialized to the task managers. Constructing one never
 * touches the network: {@link ClickHouseAsyncSinkBuilder#build()} verifies connectivity on the
 * job driver via {@link #verifyConnectivity()}, while the Table API factory relies on its
 * planning-time DESCRIBE through {@link #createPlanningClient(Map)} failing instead.
 */
public class ClickHouseClientConfig implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseClientConfig.class);
    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_MAX_RETRIES = 3;

    private final String url;
    private final String username;
    private final String password;
    private final String database;
    private final String tableName;
    private final String fullProductName;
    private Boolean supportDefault = null;
    private final Map<String, String> options;
    private final Map<String, String> serverSettings;
    private boolean enableJsonSupportAsString;
    private transient Client client = null;
    private RetryPolicy retryPolicy = RetryPolicy.forever();
    private BatchFailureStrategy batchFailureStrategy = BatchFailureStrategy.STOP_FLINK;

    public ClickHouseClientConfig(String url, String username, String password, String database, String tableName, Map<String, String> options, Map<String, String> serverSettings, boolean enableJsonSupportAsString) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.database = database;
        this.tableName = tableName;
        this.fullProductName = String.format("Flink-ClickHouse-Sink/%s (fv:flink/%s, lv:scala/%s)", ClickHouseSinkVersion.getVersion(), EnvironmentInformation.getVersion(), EnvironmentInformation.getScalaVersion());
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

    /** Deep copy for DynamicTableSink#copy(); the cached client is not shared. */
    public ClickHouseClientConfig copy() {
        ClickHouseClientConfig copy = new ClickHouseClientConfig(
                url, username, password, database, tableName, options, serverSettings, enableJsonSupportAsString);
        copy.setSupportDefault(supportDefault);
        copy.setRetryPolicy(retryPolicy);
        copy.setBatchFailureStrategy(batchFailureStrategy);
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
        try (Client probe = initClient(database, Map.of())) {
            boolean isServerAlive = false;
            for (int i = 0; i < DEFAULT_MAX_RETRIES && !isServerAlive; i++) {
                isServerAlive = probe.ping();
                if (!isServerAlive) {
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
        return initClient(database, planningServerSettings);
    }

    private Client initClient(String database, Map<String, String> extraServerSettings) {
        Client.Builder clientBuilder = new Client.Builder()
                .addEndpoint(url)
                .setUsername(username)
                .setPassword(password)
                .setDefaultDatabase(database)
                .setClientName(fullProductName)
                .setOption(ClientConfigProperties.ASYNC_OPERATIONS.getKey(), "true")
                .setOptions(options);

        for (Map.Entry<String, String> entry : serverSettings.entrySet()) {
            clientBuilder.serverSetting(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, String> entry : extraServerSettings.entrySet()) {
            clientBuilder.serverSetting(entry.getKey(), entry.getValue());
        }
        return clientBuilder.build();
    }

    public Client createClient(String database) {
        if (this.client == null) {
            this.client = initClient(database, Map.of());
        }
        return client;
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

}
