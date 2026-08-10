package org.apache.flink.connector.clickhouse.table.schema;

import java.time.ZoneId;
import java.util.Objects;

/** The connector options schema resolution depends on, plus context for error messages. */
public final class SchemaResolverOptions {

    public final String database;
    public final String table;
    public final ZoneId sinkTimezone;
    public final boolean ignoreUnknownFlinkColumns;

    public SchemaResolverOptions(String database, String table, ZoneId sinkTimezone,
                                 boolean ignoreUnknownFlinkColumns) {
        this.database = Objects.requireNonNull(database, "database");
        this.table = Objects.requireNonNull(table, "table");
        this.sinkTimezone = Objects.requireNonNull(sinkTimezone, "sinkTimezone");
        this.ignoreUnknownFlinkColumns = ignoreUnknownFlinkColumns;
    }
}
