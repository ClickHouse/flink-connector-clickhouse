package org.apache.flink.connector.clickhouse.table.schema;

import java.time.ZoneId;
import java.util.Objects;

/** The connector options schema resolution depends on, plus context for error messages. */
public final class SchemaResolverOptions {

    public final String database;
    /** The table name as written in the 'table' option, unquoted; the sink quotes it for SQL. */
    public final String table;
    public final ZoneId sinkTimezone;
    public final boolean ignoreUnknownFlinkColumns;
    /** {@code sink.strict-numeric-mapping}: reject at planning every numeric pair whose values would need a per-record range check. */
    public final boolean strictNumericMapping;

    public SchemaResolverOptions(String database, String table, ZoneId sinkTimezone,
                                 boolean ignoreUnknownFlinkColumns) {
        this(database, table, sinkTimezone, ignoreUnknownFlinkColumns, false);
    }

    public SchemaResolverOptions(String database, String table, ZoneId sinkTimezone,
                                 boolean ignoreUnknownFlinkColumns, boolean strictNumericMapping) {
        this.database = Objects.requireNonNull(database, "database");
        this.table = Objects.requireNonNull(table, "table");
        this.sinkTimezone = Objects.requireNonNull(sinkTimezone, "sinkTimezone");
        this.ignoreUnknownFlinkColumns = ignoreUnknownFlinkColumns;
        this.strictNumericMapping = strictNumericMapping;
    }

    public TypeMappingOptions typeMapping() {
        return new TypeMappingOptions(sinkTimezone, strictNumericMapping);
    }
}
