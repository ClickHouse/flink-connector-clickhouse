package org.apache.flink.connector.clickhouse.table;

import java.util.Objects;

/** Quoting for names the connector puts into SQL text itself. */
public final class ClickHouseIdentifiers {

    private ClickHouseIdentifiers() {}

    /**
     * Wraps a name in backquotes, escaping a backslash or backquote inside it, so any table name
     * ClickHouse accepts ({@code my-table}, {@code 2024_events}, a name with a space or a dot)
     * reads as one identifier. client-v2 puts the table name into {@code DESCRIBE TABLE} and
     * {@code INSERT INTO} exactly as given (clickhouse-java#3089), so quoting is the caller's job.
     */
    public static String quote(String name) {
        Objects.requireNonNull(name, "name");
        return '`' + name.replace("\\", "\\\\").replace("`", "\\`") + '`';
    }
}
