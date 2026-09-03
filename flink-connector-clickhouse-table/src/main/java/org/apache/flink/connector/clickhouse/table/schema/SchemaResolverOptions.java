package org.apache.flink.connector.clickhouse.table.schema;

import org.apache.flink.table.api.ValidationException;

import java.time.ZoneId;
import java.util.Objects;
import java.util.regex.Pattern;

/** The connector options schema resolution depends on, plus context for error messages. */
public final class SchemaResolverOptions {

    /** Names ClickHouse SQL accepts without backquotes. */
    private static final Pattern UNQUOTED_IDENTIFIER = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");

    public final String database;
    public final String table;
    public final ZoneId sinkTimezone;
    public final boolean ignoreUnknownFlinkColumns;

    public SchemaResolverOptions(String database, String table, ZoneId sinkTimezone,
                                 boolean ignoreUnknownFlinkColumns) {
        this.database = Objects.requireNonNull(database, "database");
        this.table = requireUnquotedTableName(table);
        this.sinkTimezone = Objects.requireNonNull(sinkTimezone, "sinkTimezone");
        this.ignoreUnknownFlinkColumns = ignoreUnknownFlinkColumns;
    }

    /**
     * client-v2 concatenates the table name unquoted into DESCRIBE TABLE and INSERT INTO
     * (https://github.com/ClickHouse/clickhouse-java/issues/3089); the database travels as
     * a request setting and needs no check.
     */
    private static String requireUnquotedTableName(String table) {
        Objects.requireNonNull(table, "table");
        if (UNQUOTED_IDENTIFIER.matcher(table).matches()) {
            return table;
        }
        throw new ValidationException(String.format(
                "The 'table' option value '%s' needs quoting in SQL, which the connector does not "
                + "support: the ClickHouse client concatenates the table name unquoted into "
                + "DESCRIBE TABLE and INSERT INTO statements, so only names matching "
                + "[a-zA-Z_][a-zA-Z0-9_]* can be used.", table));
    }
}
