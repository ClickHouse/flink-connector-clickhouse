package org.apache.flink.connector.clickhouse.introspection;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.metadata.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Planning-time lookup of a ClickHouse table's real column types via client-v2's
 * {@link Client#getTableSchema(String, String)}. Deliberately unmemoized: planning must
 * validate against the table as it exists now, and a JVM-wide memo goes stale after
 * {@code ALTER TABLE} in a long-lived planner (SQL gateway, session cluster). {@code EXPLAIN},
 * statement sets and {@code EXECUTE PLAN} re-invoke the table factory, so each invocation
 * costs one short-lived client — created by the supplier (connectivity checks belong inside
 * it), closed before returning.
 *
 * <p>Lives in {@code -base}, which stays Flink-free.
 */
public final class TableIntrospector {
    private static final Logger LOG = LoggerFactory.getLogger(TableIntrospector.class);

    private TableIntrospector() {}

    /**
     * Returns the current schema of {@code database.table}, read through the client the
     * supplier creates; the client is closed before returning.
     */
    public static TableSchema introspect(String url, String database, String table,
                                         Supplier<Client> clientSupplier) {
        Objects.requireNonNull(url, "url");
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(clientSupplier, "clientSupplier");
        LOG.info("Introspecting ClickHouse table {}.{} at {}", database, table, url);
        try (Client client = clientSupplier.get()) {
            return client.getTableSchema(table, database);
        }
    }
}
