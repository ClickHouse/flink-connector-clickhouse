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
 * Flink SQL factory for {@code 'connector' = 'clickhouse'}: validate options → ping →
 * introspect the target table → resolve the schema → build the sink. All option, schema
 * and connectivity errors surface here, at planning time.
 *
 * <p>Re-invoked by {@code EXPLAIN}, statement sets and {@code EXECUTE PLAN} — each
 * invocation pings and introspects anew through a short-lived client, so planning always
 * validates against the table's current schema, even after {@code ALTER TABLE} in a
 * long-lived planner JVM (SQL gateway, session cluster).
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
        ReadableConfig options = validatedOptions(context);
        validateBatchingOptions(options);
        // Built first so an invalid sink.timezone or unsupported table name fails before any network call.
        SchemaResolverOptions resolverOptions = buildResolverOptions(options);
        logIgnoredPrimaryKey(context);

        ClickHouseClientConfig clientConfig = buildClientConfig(context, options);
        List<ResolvedColumnMapping> mappings = SchemaResolver.resolve(
                context.getCatalogTable().getResolvedSchema(),
                introspect(options, clientConfig),
                resolverOptions);
        // Servers too old to know input_format_binary_read_json_as_string never see it.
        clientConfig.setEnableJsonSupportAsString(SchemaResolver.targetsJsonColumn(mappings));

        return buildSink(clientConfig, RowDataDataMapper.of(mappings), options);
    }

    // ------------------------------------------------------------------------------------
    // Steps
    // ------------------------------------------------------------------------------------

    private ReadableConfig validatedOptions(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(CLIENT_OPTIONS_PREFIX, SERVER_SETTINGS_PREFIX);
        return helper.getOptions();
    }

    /** The AsyncSink writer re-checks these at task start; failing here names the SQL options instead. */
    private static void validateBatchingOptions(ReadableConfig options) {
        int maxRows = options.get(SINK_BUFFER_FLUSH_MAX_ROWS);
        long maxBytes = options.get(SINK_BUFFER_FLUSH_MAX_BYTES).getBytes();
        int maxBuffered = options.get(SINK_MAX_BUFFERED_REQUESTS);
        long recordMaxBytes = options.get(SINK_RECORD_MAX_BYTES).getBytes();

        requirePositive(SINK_BUFFER_FLUSH_MAX_ROWS.key(), maxRows);
        requirePositive(SINK_BUFFER_FLUSH_MAX_BYTES.key(), maxBytes);
        requirePositive(SINK_BUFFER_FLUSH_INTERVAL.key(), options.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis());
        requirePositive(SINK_MAX_IN_FLIGHT_REQUESTS.key(), options.get(SINK_MAX_IN_FLIGHT_REQUESTS));
        requirePositive(SINK_MAX_BUFFERED_REQUESTS.key(), maxBuffered);
        requirePositive(SINK_RECORD_MAX_BYTES.key(), recordMaxBytes);
        if (maxBuffered <= maxRows) {
            throw new ValidationException(String.format(
                    "'%s' (%d) must be strictly greater than '%s' (%d).",
                    SINK_MAX_BUFFERED_REQUESTS.key(), maxBuffered,
                    SINK_BUFFER_FLUSH_MAX_ROWS.key(), maxRows));
        }
        if (maxBytes < recordMaxBytes) {
            throw new ValidationException(String.format(
                    "'%s' (%d bytes) must be at least '%s' (%d bytes).",
                    SINK_BUFFER_FLUSH_MAX_BYTES.key(), maxBytes,
                    SINK_RECORD_MAX_BYTES.key(), recordMaxBytes));
        }
    }

    private static void requirePositive(String key, long value) {
        if (value <= 0) {
            throw new ValidationException(
                    String.format("'%s' must be positive, but was %d.", key, value));
        }
    }

    private static SchemaResolverOptions buildResolverOptions(ReadableConfig options) {
        return new SchemaResolverOptions(
                options.get(DATABASE),
                options.get(TABLE),
                parseSinkTimezone(options.get(SINK_TIMEZONE)),
                options.get(SINK_IGNORE_UNKNOWN_FLINK_COLUMNS));
    }

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
                toRetryPolicy(options.get(SINK_MAX_RETRIES)));
        clientConfig.setBatchFailureStrategy(
                parseBatchFailureStrategy(options.get(SINK_BATCH_FAILURE_STRATEGY)));
        return clientConfig;
    }

    /** Reads the table's current column types through a short-lived, pinged client. */
    private static TableSchema introspect(ReadableConfig options, ClickHouseClientConfig clientConfig) {
        String url = options.get(URL);
        String database = options.get(DATABASE);
        String table = options.get(TABLE);
        try {
            return TableIntrospector.introspect(url, database, table,
                    clientConfig::createPlanningClient);
        } catch (Exception e) {
            throw new ValidationException(String.format(
                    "Could not read the schema of ClickHouse table %s.%s at %s — %s",
                    database, table, url, e.getMessage() != null ? e.getMessage() : e.toString()), e);
        }
    }

    private static ClickHouseDynamicTableSink buildSink(ClickHouseClientConfig clientConfig,
                                                       RowDataDataMapper mapper,
                                                       ReadableConfig options) {
        return new ClickHouseDynamicTableSink(
                clientConfig,
                mapper,
                options.get(SINK_BUFFER_FLUSH_MAX_ROWS),
                options.get(SINK_BUFFER_FLUSH_MAX_BYTES).getBytes(),
                options.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis(),
                options.get(SINK_MAX_IN_FLIGHT_REQUESTS),
                options.get(SINK_MAX_BUFFERED_REQUESTS),
                options.get(SINK_RECORD_MAX_BYTES).getBytes(),
                options.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null),
                options.get(DATABASE) + "." + options.get(TABLE));
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

    static RetryPolicy toRetryPolicy(int maxRetries) {
        if (maxRetries == -1) {
            return RetryPolicy.forever();
        }
        if (maxRetries < 0) {
            throw new ValidationException(String.format(
                    "'%s' must be >= 0, or -1 for unlimited retries, but was %d.",
                    SINK_MAX_RETRIES.key(), maxRetries));
        }
        return RetryPolicy.limited(maxRetries);
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
