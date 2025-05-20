package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class ClickHouseClientConfig implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseClientConfig.class);

    private final String url;
    private final String username;
    private final String password;
    private final String database;
    private final String tableName;

    public ClickHouseClientConfig(String url, String username, String password, String database, String tableName) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.database = database;
        this.tableName = tableName;
    }

    public Client createClient(String database) {
        return new Client.Builder()
                .addEndpoint(url)
                .setUsername(username)
                .setPassword(password)
                .setDefaultDatabase(database)
                .setOption(ClientConfigProperties.ASYNC_OPERATIONS.getKey(), "true")
                .build();
    }

    public Client createClient() {
        return createClient(this.database);
    }

    public String getTableName() { return tableName; }


}
