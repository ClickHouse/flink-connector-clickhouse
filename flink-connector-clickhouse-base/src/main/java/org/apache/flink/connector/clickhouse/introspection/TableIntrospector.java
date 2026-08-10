package org.apache.flink.connector.clickhouse.introspection;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.metadata.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * Planning-time lookup of a ClickHouse table's real column types via client-v2's
 * {@link Client#getTableSchema(String, String)}, memoized per {@code (url, database, table)}:
 * {@code EXPLAIN}, statement sets and {@code EXECUTE PLAN} re-invoke the table factory and
 * must not ping/introspect anew. The client supplier runs only on a cache miss —
 * connectivity checks belong inside it.
 *
 * <p>Lives in {@code -base}, which stays Flink-free (documented invariant).
 */
public final class TableIntrospector {
    private static final Logger LOG = LoggerFactory.getLogger(TableIntrospector.class);

    private static final ConcurrentMap<String, TableSchema> CACHE = new ConcurrentHashMap<>();

    private TableIntrospector() {}

    /**
     * Returns the schema of {@code database.table} — through the supplied client on first
     * use, from the memo on every re-invocation with the same {@code (url, database, table)}.
     */
    public static TableSchema introspect(String url, String database, String table,
                                         Supplier<Client> clientSupplier) {
        Objects.requireNonNull(url, "url");
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(clientSupplier, "clientSupplier");
        return CACHE.computeIfAbsent(cacheKey(url, database, table), key -> {
            LOG.info("Introspecting ClickHouse table {}.{} at {}", database, table, url);
            return clientSupplier.get().getTableSchema(table, database);
        });
    }

    /** Drops all memoized schemas — intended for tests. */
    public static void clearCache() {
        CACHE.clear();
    }

    private static String cacheKey(String url, String database, String table) {
        // NUL (octal escape) cannot appear in identifiers, so the key is collision-free.
        return url + '\0' + database + '\0' + table;
    }
}
