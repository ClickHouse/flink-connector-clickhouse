package org.apache.flink.connector.clickhouse.table.catalog;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A table of {@link ClickHouseCatalog}. Flink's own implementation is out of reach here:
 * {@code CatalogTable.of} ends with Flink 1.20 and {@code CatalogTable.newBuilder} starts there.
 */
final class ClickHouseCatalogTable implements CatalogTable {

    private final Schema schema;
    private final Map<String, String> options;

    ClickHouseCatalogTable(Schema schema, Map<String, String> options) {
        this.schema = Objects.requireNonNull(schema, "schema");
        this.options = Collections.unmodifiableMap(new HashMap<>(options));
    }

    @Override
    public Schema getUnresolvedSchema() {
        return schema;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public String getComment() {
        return "";
    }

    @Override
    public boolean isPartitioned() {
        return false;
    }

    @Override
    public List<String> getPartitionKeys() {
        return Collections.emptyList();
    }

    @Override
    public CatalogBaseTable copy() {
        return this;
    }

    /** The planner's hook for {@code OPTIONS} hints. */
    @Override
    public CatalogTable copy(Map<String, String> newOptions) {
        return new ClickHouseCatalogTable(schema, newOptions);
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.empty();
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.empty();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ClickHouseCatalogTable)) {
            return false;
        }
        ClickHouseCatalogTable that = (ClickHouseCatalogTable) other;
        return schema.equals(that.schema) && options.equals(that.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, options);
    }
}
