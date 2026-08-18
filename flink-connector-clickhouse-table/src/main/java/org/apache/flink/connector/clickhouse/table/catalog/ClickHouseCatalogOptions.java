package org.apache.flink.connector.clickhouse.table.catalog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions;

import java.util.HashMap;
import java.util.Map;

/**
 * The {@code WITH (...)} options of {@code CREATE CATALOG ... ('type' = 'clickhouse')}.
 *
 * <p>The connection keys ({@code url}, {@code username}, {@code password}) are shared with
 * the sink's {@link ClickHouseConnectorOptions}; this class adds the catalog-only key and
 * the rule for which catalog options are forwarded into every table the catalog serves.
 */
public final class ClickHouseCatalogOptions {

    private ClickHouseCatalogOptions() {}

    public static final ConfigOption<String> DEFAULT_DATABASE = ConfigOptions
            .key("default-database")
            .stringType()
            .defaultValue("default")
            .withDescription("Database used when a statement does not qualify the table with one.");

    /** {@code sink.<key>} catalog options become {@code sink.<key>} options on every table. */
    public static final String SINK_OPTIONS_PREFIX = "sink.";

    /**
     * Catalog options copied verbatim into each emitted table's options: the sink's batching
     * and reliability knobs plus the {@code clickhouse.client.*}/{@code clickhouse.server.*}
     * passthroughs — set once at catalog level, effective for every table written through it.
     */
    public static Map<String, String> forwardedTableOptions(Map<String, String> catalogOptions) {
        Map<String, String> forwarded = new HashMap<>();
        catalogOptions.forEach((key, value) -> {
            if (key.startsWith(SINK_OPTIONS_PREFIX)
                    || key.startsWith(ClickHouseConnectorOptions.CLIENT_OPTIONS_PREFIX)
                    || key.startsWith(ClickHouseConnectorOptions.SERVER_SETTINGS_PREFIX)) {
                forwarded.put(key, value);
            }
        });
        return forwarded;
    }
}
