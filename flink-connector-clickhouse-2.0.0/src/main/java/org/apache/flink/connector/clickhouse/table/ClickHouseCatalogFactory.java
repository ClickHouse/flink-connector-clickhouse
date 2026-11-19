package org.apache.flink.connector.clickhouse.table;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientMisconfigurationException;
import com.clickhouse.client.api.metadata.TableSchema;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.connector.clickhouse.table.catalog.ClickHouseCatalog;
import org.apache.flink.connector.clickhouse.table.catalog.ClickHouseMetadataClient;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.CLIENT_OPTIONS_PREFIX;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.DATABASE;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.SERVER_SETTINGS_PREFIX;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.SINK_TIMEZONE;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.TABLE;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.URL;
import static org.apache.flink.connector.clickhouse.table.catalog.ClickHouseCatalogOptions.DEFAULT_DATABASE;

/**
 * Flink SQL factory for {@code CREATE CATALOG ... WITH ('type' = 'clickhouse')}. Besides the
 * connection it takes every option of the SQL sink, checks them as the sink factory would and
 * hands them to each table the catalog returns.
 */
public class ClickHouseCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = ClickHouseDynamicTableSinkFactory.IDENTIFIER;

    /** Always there, so the catalog's own queries depend on no user database. */
    private static final String METADATA_DATABASE = "system";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new ClickHouseDynamicTableSinkFactory().requiredOptions();
        // The catalog fills these two in per table.
        options.remove(DATABASE);
        options.remove(TABLE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new ClickHouseDynamicTableSinkFactory().optionalOptions();
        options.add(DEFAULT_DATABASE);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        FactoryUtil.CatalogFactoryHelper helper = FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validateExcept(CLIENT_OPTIONS_PREFIX, SERVER_SETTINGS_PREFIX);

        Map<String, String> tableOptions = tableOptions(context.getOptions());
        Map<String, String> metadataOptions = new HashMap<>(tableOptions);
        metadataOptions.put(DATABASE.key(), METADATA_DATABASE);
        validateAsTableOptions(metadataOptions);

        return new ClickHouseCatalog(context.getName(), helper.getOptions().get(DEFAULT_DATABASE),
                tableOptions, new PlanningMetadataClient(metadataOptions), new ClickHouseDynamicTableSinkFactory());
    }

    /** What every table of the catalog is returned with, short of its own database and table. */
    static Map<String, String> tableOptions(Map<String, String> catalogOptions) {
        Map<String, String> tableOptions = new HashMap<>(catalogOptions);
        tableOptions.remove(DEFAULT_DATABASE.key());
        tableOptions.put(FactoryUtil.CONNECTOR.key(), ClickHouseDynamicTableSinkFactory.IDENTIFIER);
        return tableOptions;
    }

    /** The sink factory's own checks, so a bad option fails at CREATE CATALOG, not at the first INSERT. */
    private static void validateAsTableOptions(Map<String, String> tableOptions) {
        Configuration options = Configuration.fromMap(tableOptions);
        ClickHouseDynamicTableSinkFactory.validateBatchingOptions(options);
        ClickHouseDynamicTableSinkFactory.validateUrl(options.get(URL));
        ClickHouseDynamicTableSinkFactory.parseSinkTimezone(options.get(SINK_TIMEZONE));
        ClickHouseDynamicTableSinkFactory.buildClientConfig(tableOptions, options);
    }

    /** The catalog's reads, made through the sink factory's planning client and introspection. */
    private static final class PlanningMetadataClient implements ClickHouseMetadataClient {

        private final Map<String, String> metadataOptions;

        private PlanningMetadataClient(Map<String, String> metadataOptions) {
            this.metadataOptions = metadataOptions;
        }

        @Override
        public List<String> queryColumn(String sql, Map<String, Object> parameters) {
            Configuration options = Configuration.fromMap(metadataOptions);
            ClickHouseClientConfig clientConfig =
                    ClickHouseDynamicTableSinkFactory.buildClientConfig(metadataOptions, options);
            Client client;
            try {
                client = clientConfig.createPlanningClient(Map.of());
            } catch (IllegalArgumentException | ClientMisconfigurationException e) {
                // build() alone, worded as the sink factory words it: a rejected option is not a failed read.
                throw new ValidationException(String.format(
                        "Invalid '%s*' option or option combination — the ClickHouse client rejected it: %s",
                        CLIENT_OPTIONS_PREFIX, withCause(e)), e);
            }
            try (Client metadataClient = client) {
                return metadataClient.queryAll(sql, parameters).stream()
                        .map(row -> row.getString(1))
                        .collect(Collectors.toList());
            } catch (Exception e) {
                throw new CatalogException(String.format(
                        "Could not read the metadata of ClickHouse at %s — %s",
                        options.get(URL), ClickHouseDynamicTableSinkFactory.rootMessage(e)), e);
            }
        }

        /** build() names an unreadable SSL file or bad password only in a cause. */
        private static String withCause(Throwable e) {
            return e.getCause() == null ? e.getMessage()
                    : e.getMessage() + " (" + ClickHouseDynamicTableSinkFactory.rootMessage(e.getCause()) + ")";
        }

        @Override
        public TableSchema describe(Map<String, String> tableOptions) {
            Configuration options = Configuration.fromMap(tableOptions);
            return ClickHouseDynamicTableSinkFactory.introspect(
                    options, ClickHouseDynamicTableSinkFactory.buildClientConfig(tableOptions, options));
        }
    }
}
