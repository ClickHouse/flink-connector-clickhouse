package org.apache.flink.connector.clickhouse.table.catalog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CommonCatalogOptions;

/**
 * The options {@code CREATE CATALOG ... WITH ('type' = 'clickhouse')} adds to the connection and
 * sink options of {@code ClickHouseConnectorOptions}, which the catalog hands on to its tables.
 */
public final class ClickHouseCatalogOptions {

    private ClickHouseCatalogOptions() {}

    public static final ConfigOption<String> DEFAULT_DATABASE = ConfigOptions
            .key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
            .stringType()
            .defaultValue("default")
            .withDescription("ClickHouse database that is current after USE CATALOG; it must exist.");
}
