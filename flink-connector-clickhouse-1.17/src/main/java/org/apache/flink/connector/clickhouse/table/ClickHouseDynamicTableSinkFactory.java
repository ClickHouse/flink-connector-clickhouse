package org.apache.flink.connector.clickhouse.table;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.config.BatchFailureStrategy;
import com.clickhouse.config.RetryPolicy;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.clickhouse.introspection.TableIntrospector;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.connector.clickhouse.table.data.RowDataDataMapper;
import org.apache.flink.connector.clickhouse.table.schema.ResolvedColumnMapping;
import org.apache.flink.connector.clickhouse.table.schema.SchemaResolver;
import org.apache.flink.connector.clickhouse.table.schema.SchemaResolverOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.CLIENT_OPTIONS_PREFIX;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.DATABASE;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.PASSWORD;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.SERVER_SETTINGS_PREFIX;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.SINK_BATCH_FAILURE_STRATEGY;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.SINK_BUFFER_FLUSH_MAX_BYTES;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.SINK_IGNORE_UNKNOWN_FLINK_COLUMNS;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.SINK_MAX_BUFFERED_REQUESTS;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.SINK_MAX_IN_FLIGHT_REQUESTS;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.SINK_RECORD_MAX_BYTES;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.SINK_TIMEZONE;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.TABLE;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.URL;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.USERNAME;

/**
 * Flink SQL factory for {@code 'connector' = 'clickhouse'}
 * (docs/table-api/dld-ClickHouseDynamicTableSinkFactory.md):
 * validate options → build {@link ClickHouseClientConfig} → {@code pingWithRetry()} →
 * introspect the target table → resolve the schema → build the sink. All option, schema
 * and connectivity errors surface here, at planning time.
 *
 * <p>Re-invoked by {@code EXPLAIN}, statement sets and {@code EXECUTE PLAN} —
 * ping + introspection are memoized per {@code (url, database, table)} inside
 * {@link TableIntrospector}.
 */
public class ClickHouseDynamicTableSinkFactory implements DynamicTableSinkFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseDynamicTableSinkFactory.class);

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
        options.add(DATABASE);
        options.add(TABLE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PASSWORD);
        options.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        options.add(SINK_BUFFER_FLUSH_MAX_BYTES);
        options.add(SINK_BUFFER_FLUSH_INTERVAL);
        options.add(SINK_MAX_IN_FLIGHT_REQUESTS);
        options.add(SINK_MAX_BUFFERED_REQUESTS);
        options.add(SINK_RECORD_MAX_BYTES);
        options.add(FactoryUtil.SINK_PARALLELISM);
        options.add(SINK_MAX_RETRIES);
        options.add(SINK_BATCH_FAILURE_STRATEGY);
        options.add(SINK_TIMEZONE);
        options.add(SINK_IGNORE_UNKNOWN_FLINK_COLUMNS);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(CLIENT_OPTIONS_PREFIX, SERVER_SETTINGS_PREFIX);
        ReadableConfig options = helper.getOptions();

        ZoneId sinkTimezone = parseSinkTimezone(options.get(SINK_TIMEZONE));
        logIgnoredPrimaryKey(context);

        ClickHouseClientConfig clientConfig = buildClientConfig(context, options);
        TableSchema clickHouseSchema = introspect(options, clientConfig);
        List<ResolvedColumnMapping> mappings = SchemaResolver.resolve(
                context.getCatalogTable().getResolvedSchema(),
                clickHouseSchema,
                new SchemaResolverOptions(
                        options.get(DATABASE), options.get(TABLE), sinkTimezone,
                        options.get(SINK_IGNORE_UNKNOWN_FLINK_COLUMNS)));

        // JSON auto-enable: send input_format_binary_read_json_as_string exactly when a
        // JSON column is mapped — servers too old to know the setting never see it.
        clientConfig.setEnableJsonSupportAsString(SchemaResolver.targetsJsonColumn(mappings));

        return new ClickHouseDynamicTableSink(
                clientConfig,
                RowDataDataMapper.of(mappings),
                options.get(SINK_BUFFER_FLUSH_MAX_ROWS),
                options.get(SINK_BUFFER_FLUSH_MAX_BYTES).getBytes(),
                options.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis(),
                options.get(SINK_MAX_IN_FLIGHT_REQUESTS),
                options.get(SINK_MAX_BUFFERED_REQUESTS),
                options.get(SINK_RECORD_MAX_BYTES).getBytes(),
                options.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null),
                options.get(DATABASE) + "." + options.get(TABLE));
    }

    // ------------------------------------------------------------------------------------
    // Steps
    // ------------------------------------------------------------------------------------

    private static ClickHouseClientConfig buildClientConfig(Context context, ReadableConfig options) {
        Map<String, String> tableOptions = context.getCatalogTable().getOptions();
        ClickHouseClientConfig clientConfig = new ClickHouseClientConfig(
                options.get(URL),
                options.get(USERNAME),
                options.get(PASSWORD),
                options.get(DATABASE),
                options.get(TABLE),
                prefixedOptions(tableOptions, CLIENT_OPTIONS_PREFIX),
                prefixedOptions(tableOptions, SERVER_SETTINGS_PREFIX),
                retryPolicy(options.get(SINK_MAX_RETRIES)));
        clientConfig.setBatchFailureStrategy(
                parseBatchFailureStrategy(options.get(SINK_BATCH_FAILURE_STRATEGY)));
        return clientConfig;
    }

    /**
     * Reads the target table's real column types. The supplier — ping first, then
     * introspect — runs only on a memoization miss, so {@code EXPLAIN}/statement-set
     * re-invocations stay offline.
     */
    private static TableSchema introspect(ReadableConfig options, ClickHouseClientConfig clientConfig) {
        String url = options.get(URL);
        String database = options.get(DATABASE);
        String table = options.get(TABLE);
        try {
            return TableIntrospector.introspect(url, database, table, () -> {
                clientConfig.pingWithRetry();
                return clientConfig.createClient();
            });
        } catch (Exception e) {
            throw new ValidationException(String.format(
                    "Could not read the schema of ClickHouse table %s.%s at %s — %s",
                    database, table, url, e.getMessage()), e);
        }
    }

    private static void logIgnoredPrimaryKey(Context context) {
        context.getCatalogTable().getResolvedSchema().getPrimaryKey().ifPresent(pk ->
                LOG.info("PRIMARY KEY {} on table {} is accepted and ignored — the ClickHouse sink "
                        + "is insert-only and does not enforce keys.",
                        pk.getColumns(), context.getObjectIdentifier()));
    }

    // ------------------------------------------------------------------------------------
    // Option parsing
    // ------------------------------------------------------------------------------------

    private static ZoneId parseSinkTimezone(String zone) {
        try {
            return ZoneId.of(zone);
        } catch (DateTimeException e) {
            throw new ValidationException(String.format(
                    "Invalid value '%s' for '%s': %s", zone, SINK_TIMEZONE.key(), e.getMessage()), e);
        }
    }

    private static RetryPolicy retryPolicy(int maxRetries) {
        return maxRetries < 0 ? RetryPolicy.forever() : RetryPolicy.limited(maxRetries);
    }

    private static BatchFailureStrategy parseBatchFailureStrategy(String value) {
        try {
            return BatchFailureStrategy.valueOf(
                    value.trim().toUpperCase(Locale.ROOT).replace('-', '_'));
        } catch (IllegalArgumentException e) {
            throw new ValidationException(String.format(
                    "Invalid value '%s' for '%s' — supported values: 'stop-flink', 'drop-batch'.",
                    value, SINK_BATCH_FAILURE_STRATEGY.key()));
        }
    }

    /** Collects {@code <prefix><key> = value} table options into a {@code key -> value} map. */
    private static Map<String, String> prefixedOptions(Map<String, String> tableOptions, String prefix) {
        Map<String, String> extracted = new HashMap<>();
        tableOptions.forEach((key, value) -> {
            if (key.startsWith(prefix)) {
                extracted.put(key.substring(prefix.length()), value);
            }
        });
        return extracted;
    }
}
