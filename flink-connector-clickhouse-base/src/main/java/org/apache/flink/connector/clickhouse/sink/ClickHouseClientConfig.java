package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class ClickHouseClientConfig implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseClientConfig.class);
    private static final long serialVersionUID = 1L;

    private final String url;
    private final String username;
    private final String password;
    private final String database;
    private final String tableName;
    private final String fullProductName;
    private Boolean supportDefault = null;

    public ClickHouseClientConfig(String url, String username, String password, String database, String tableName) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.database = database;
        this.tableName = tableName;
        this.fullProductName = String.format("Flink-ClickHouse-Sink/%s (fv:flink/%s, lv:scala/%s)", ClickHouseSinkVersion.getVersion(), EnvironmentInformation.getVersion(), EnvironmentInformation.getScalaVersion());
    }

    public Client createClient(String database) {
        Client client = new Client.Builder()
                .addEndpoint(url)
                .setUsername(username)
                .setPassword(password)
                .setDefaultDatabase(database)
                .setClientName(fullProductName)
                .setOption(ClientConfigProperties.ASYNC_OPERATIONS.getKey(), "true")
                .build();
        return client;
    }

    public Client createClient() {
        return createClient(this.database);
    }

    public String getTableName() { return tableName; }

    public void setSupportDefault(Boolean supportDefault) {
        this.supportDefault = supportDefault;
    }

    public Boolean getSupportDefault() {
        return supportDefault;
    }
}
