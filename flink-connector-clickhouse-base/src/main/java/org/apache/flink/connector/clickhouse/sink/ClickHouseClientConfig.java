package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
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
//    private List<Class<?>> classToReisterList = null;
//    private List<TableSchema> tableSchemaList = null;

    public ClickHouseClientConfig(String url, String username, String password, String database, String tableName) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.database = database;
        this.tableName = tableName;
//        this.classToReisterList = new ArrayList<>();
//        this.tableSchemaList = new ArrayList<>();
    }

    public Client createClient(String database) {
        Client client = new Client.Builder()
                .addEndpoint(url)
                .setUsername(username)
                .setPassword(password)
                .setDefaultDatabase(database)
                .setOption(ClientConfigProperties.ASYNC_OPERATIONS.getKey(), "true")
                .build();
//        if (classToReisterList != null) {
//            for (int index = 0; index < classToReisterList.size(); index++) {
//                client.register(classToReisterList.get(index), tableSchemaList.get(index));
//            }
//        }
        return client;
    }

//    public void registerClass(Class<?> clazz, TableSchema tableSchema) {
//        classToReisterList.add(clazz);
//        tableSchemaList.add(tableSchema);
//    }

    public Client createClient() {
        return createClient(this.database);
    }

    public String getTableName() { return tableName; }


}
