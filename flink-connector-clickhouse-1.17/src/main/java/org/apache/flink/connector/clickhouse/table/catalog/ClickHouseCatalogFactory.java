package org.apache.flink.connector.clickhouse.table.catalog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.CLIENT_OPTIONS_PREFIX;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.PASSWORD;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.SERVER_SETTINGS_PREFIX;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.URL;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.USERNAME;
import static org.apache.flink.connector.clickhouse.table.catalog.ClickHouseCatalogOptions.DEFAULT_DATABASE;
import static org.apache.flink.connector.clickhouse.table.catalog.ClickHouseCatalogOptions.SINK_OPTIONS_PREFIX;

/**
 * Flink factory for {@code CREATE CATALOG ... WITH ('type' = 'clickhouse')}: validates the
 * options and builds the read-only {@link ClickHouseCatalog}. Thin per-generation glue —
 * only {@link #constructCatalogTable} differs between the Flink 1.x and 2.x modules.
 */
public class ClickHouseCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = "clickhouse";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(URL);
        options.add(USERNAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PASSWORD);
        options.add(DEFAULT_DATABASE);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validateExcept(SINK_OPTIONS_PREFIX, CLIENT_OPTIONS_PREFIX, SERVER_SETTINGS_PREFIX);
        ReadableConfig options = helper.getOptions();
        return new ClickHouseCatalog(
                context.getName(),
                options.get(DEFAULT_DATABASE),
                options.get(URL),
                options.get(USERNAME),
                options.get(PASSWORD),
                ClickHouseCatalogOptions.forwardedTableOptions(context.getOptions()),
                ClickHouseCatalogFactory::constructCatalogTable);
    }

    /** Flink 1.x spelling; the 2.x module uses {@code CatalogTable.newBuilder()} instead. */
    private static CatalogTable constructCatalogTable(Schema schema, Map<String, String> options) {
        return CatalogTable.of(schema, null, Collections.emptyList(), options);
    }
}
