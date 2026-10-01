package com.example;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL sink example: a bounded datagen source writing into a ClickHouse table
 * declared with {@code 'connector' = 'clickhouse'}.
 *
 * <p>Nothing here references a connector class by name. The sink is resolved purely
 * through the {@code org.apache.flink.table.factories.Factory} service file inside the
 * published connector jar, so running this job proves the shaded artifact ships a
 * discoverable Table API factory — which the in-process tests cannot, since they run
 * off the test classpath.
 */
public class SqlSinkJob {

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        final String url = parameters.get("url");
        final String username = parameters.get("username");
        final String password = parameters.get("password");
        final String database = parameters.get("database");
        final String table = parameters.get("table", "sql_sink");
        final int records = parameters.getInt("records", 1000);

        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        // A sequence field with an end bound makes the source bounded, so the job finishes.
        env.executeSql(String.format(
                "CREATE TABLE src (id BIGINT NOT NULL) WITH ("
                        + "'connector' = 'datagen',"
                        + "'fields.id.kind' = 'sequence',"
                        + "'fields.id.start' = '1',"
                        + "'fields.id.end' = '%d')",
                records));

        env.executeSql(String.format(
                "CREATE TABLE ch_sink ("
                        + "id BIGINT NOT NULL,"
                        + "name STRING,"
                        + "amount DECIMAL(10, 2),"
                        // ClickHouse has no Nullable(Array(...)), so the array itself must be NOT NULL.
                        + "tags ARRAY<STRING> NOT NULL"
                        + ") WITH ("
                        + "'connector' = 'clickhouse',"
                        + "'url' = '%s',"
                        + "'username' = '%s',"
                        + "'password' = '%s',"
                        + "'database' = '%s',"
                        + "'table' = '%s',"
                        + "'sink.buffer-flush.interval' = '2s')",
                url, username, password, database, table));

        // No await(): /jars/run submits detached, and fetching the result there throws.
        env.executeSql(
                "INSERT INTO ch_sink SELECT id, CONCAT('name-', CAST(id AS STRING)), "
                        + "CAST(id AS DECIMAL(10, 2)), ARRAY[CONCAT('t', CAST(id AS STRING))] FROM src");
    }
}
