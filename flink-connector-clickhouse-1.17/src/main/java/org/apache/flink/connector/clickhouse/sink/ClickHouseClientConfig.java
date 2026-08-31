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
    private boolean enableJsonSupportAsString = true;
    private transient Client client = null;
    private RetryPolicy retryPolicy = RetryPolicy.forever();
    private BatchFailureStrategy batchFailureStrategy = BatchFailureStrategy.STOP_FLINK;

    public ClickHouseClientConfig(String url, String username, String password, String database, String tableName, Map<String, String> options, Map<String, String> serverSettings, boolean enableJsonSupportAsString) {
        this(url, username, password, database, tableName, options, serverSettings, RetryPolicy.forever());
        this.enableJsonSupportAsString = enableJsonSupportAsString;
        pingLoop(initClient(database));
    }

    /**
     * No-ping constructor for the Table API factory: connectivity is checked separately via
     * {@link #createPlanningClient()} on a short-lived client the factory closes after
     * introspection. The retry policy governs runtime batch retries only, never the ping.
     */
    public ClickHouseClientConfig(String url, String username, String password, String database, String tableName, Map<String, String> options, Map<String, String> serverSettings, RetryPolicy retryPolicy) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.database = database;
        this.tableName = tableName;
        this.fullProductName = String.format("Flink-ClickHouse-Sink/%s (fv:flink/%s, lv:scala/%s)", ClickHouseSinkVersion.getVersion(), EnvironmentInformation.getVersion(), EnvironmentInformation.getScalaVersion());
        this.options = new HashMap<>(Optional.ofNullable(options).orElseGet(HashMap::new));
        this.serverSettings = new HashMap<>(Optional.ofNullable(serverSettings).orElseGet(HashMap::new));
        this.enableJsonSupportAsString = false;
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
        LOG.info("ClickHouseClientConfig: url={}, user={}, password=******, database={}", url, username, database);
    }

    public ClickHouseClientConfig(String url, String username, String password, String database, String tableName) {
        this(url, username, password, database, tableName, new HashMap<>(), new HashMap<>(), false);
    }

    public ClickHouseClientConfig(String url, String username, String password, String database, String tableName, boolean enableJsonSupport) {
        this(url, username, password, database, tableName, new HashMap<>(), new HashMap<>(), enableJsonSupport);
    }

    /**
     * Builds a fresh client for planning-time use and verifies connectivity with a short
     * fixed ping. Bypasses the cached runtime client so nothing long-lived is left open
     * planner-side; the caller owns the returned client and must close it.
     */
    public Client createPlanningClient() {
        Client planningClient = initClient(database);
        try {
            pingLoop(planningClient);
        } catch (RuntimeException e) {
            planningClient.close();
            throw e;
        }
        return planningClient;
    }

    /**
     * Pings up to {@link #DEFAULT_MAX_RETRIES} times, 1s apart — a fixed bound, deliberately
     * not governed by sink.max-retries, which configures runtime batch retries: reusing it
     * here would let a batch-resilience setting block planning for minutes. An interrupt
     * stops the loop and is re-asserted for the caller.
     */
    private static void pingLoop(Client client) {
        boolean isServerAlive = false;
        for (int i = 0; i < DEFAULT_MAX_RETRIES && !isServerAlive; i++) {
            isServerAlive = client.ping();
            if (!isServerAlive) {
                LOG.warn(
                    "Ping failed; will retry up to {} times in {} seconds.",
                    DEFAULT_MAX_RETRIES, 1);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        if (!isServerAlive) {
            throw new RuntimeException("ClickHouse server is not accessible. Please check your configuration or ClickHouse server.");
        }
    }

    private Client initClient(String database) {
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
        return clientBuilder.build();
    }

    public Client createClient(String database) {
        if (this.client == null) {
            this.client = initClient(database);
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
