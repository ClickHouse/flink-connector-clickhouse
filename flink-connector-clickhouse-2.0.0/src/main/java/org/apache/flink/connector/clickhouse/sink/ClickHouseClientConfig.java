package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

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
    private transient Client client = null;
    private int numberOfRetries = DEFAULT_MAX_RETRIES;

    public ClickHouseClientConfig(String url, String username, String password, String database, String tableName, Map<String, String> options, Map<String, String> serverSettings) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.database = database;
        this.tableName = tableName;
        this.fullProductName = String.format("Flink-ClickHouse-Sink/%s (fv:flink/%s, lv:scala/%s)", ClickHouseSinkVersion.getVersion(), EnvironmentInformation.getVersion(), EnvironmentInformation.getScalaVersion());
        this.options = options;
        this.serverSettings = serverSettings;
        LOG.info("ClickHouseClientConfig: url={}, user={}, password={}, database={}", url, username, "x".repeat(password.length()), database);
        Client clientTmp = initClient(database);

        boolean isServerAlive = false;
        for (int i = 0; i < numberOfRetries && !isServerAlive; i++) {
            isServerAlive = clientTmp.ping();
            if (!isServerAlive) {
                LOG.warn("Ping failed, number of will {} retry in {} seconds.", numberOfRetries, 1);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {

                }
            }
        }
        if (!isServerAlive) {
            throw new RuntimeException("ClickHouse server is noy accessible. Please check your configuration or ClickHouse server.");
        }
    }


    public ClickHouseClientConfig(String url, String username, String password, String database, String tableName) {
        this(url, username, password, database, tableName, new HashMap<>(), new HashMap<>());
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

    public void setNumberOfRetries(int numberOfRetries) {
        this.numberOfRetries = numberOfRetries;
    }

    public int getNumberOfRetries() {
        return numberOfRetries;
    }
}
