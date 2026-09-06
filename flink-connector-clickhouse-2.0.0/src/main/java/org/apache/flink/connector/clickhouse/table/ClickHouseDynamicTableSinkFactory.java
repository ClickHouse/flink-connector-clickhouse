package org.apache.flink.connector.clickhouse.table;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientMisconfigurationException;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.http.ClickHouseHttpProto;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.config.BatchFailureStrategy;
import com.clickhouse.config.RetryPolicy;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.connector.clickhouse.table.data.RowDataDataMapper;
import org.apache.flink.connector.clickhouse.table.schema.ResolvedColumnMapping;
import org.apache.flink.connector.clickhouse.table.schema.SchemaResolver;
import org.apache.flink.connector.clickhouse.table.schema.SchemaResolverOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.CLIENT_OPTIONS_PREFIX;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.DATABASE;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.MAX_RETRIES_UNLIMITED;
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

    /** Set from the first-class options; a passthrough copy — client option, server setting or request header — would shadow or break them without naming the option. */
    private static final Map<String, ConfigOption<?>> RESERVED_CLIENT_KEYS = Map.of(
            ClientConfigProperties.DATABASE.getKey(), DATABASE,
            ClientConfigProperties.USER.getKey(), USERNAME,
            ClientConfigProperties.PASSWORD.getKey(), PASSWORD);

    /** The identity headers the client derives from those options: a user copy either replaces the connector's (database) or is dropped by the client's auth-header cleanup (user, key); header names are case-insensitive. */
    private static final Map<String, ConfigOption<?>> RESERVED_CLIENT_HEADERS = Map.of(
            ClickHouseHttpProto.HEADER_DATABASE.toLowerCase(Locale.ROOT), DATABASE,
            ClickHouseHttpProto.HEADER_DB_USER.toLowerCase(Locale.ROOT), USERNAME,
            ClickHouseHttpProto.HEADER_DB_PASSWORD.toLowerCase(Locale.ROOT), PASSWORD);

    /**
     * Sent with the planning DESCRIBE only: the introspected type text goes verbatim into every
     * RowBinaryWithNamesAndTypes header, and the server rejects a pretty-printed
     * {@code Tuple(<newline>    a Int32, …)} there with code 117.
     */
    static final String PRINT_PRETTY_TYPE_NAMES = "print_pretty_type_names";
    private static final Map<String, String> PLANNING_SERVER_SETTINGS = Map.of(PRINT_PRETTY_TYPE_NAMES, "0");

    /** Pinned per insert by ClickHouseAsyncWriter; client-v2 lets operation settings win, so a user copy would be discarded silently. */
    static final Set<String> INSERT_SERVER_SETTINGS = Set.of(
            "input_format_null_as_default",
            "input_format_defaults_for_omitted_fields",
            ServerSettings.INPUT_FORMAT_BINARY_READ_JSON_AS_STRING);

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
        // Checked first so an invalid url, sink.timezone or unsupported table name fails before any network call.
        validateUrl(options.get(URL));
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
    static void validateBatchingOptions(ReadableConfig options) {
        int maxRows = options.get(SINK_BUFFER_FLUSH_MAX_ROWS);
        long maxBytes = options.get(SINK_BUFFER_FLUSH_MAX_BYTES).getBytes();
        int maxBuffered = options.get(SINK_MAX_BUFFERED_REQUESTS);
        long recordMaxBytes = options.get(SINK_RECORD_MAX_BYTES).getBytes();

        requirePositive(SINK_BUFFER_FLUSH_MAX_ROWS.key(), maxRows);
        requirePositive(SINK_BUFFER_FLUSH_MAX_BYTES.key(), maxBytes);
        requireWholeMillis(SINK_BUFFER_FLUSH_INTERVAL.key(), options.get(SINK_BUFFER_FLUSH_INTERVAL));
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

    /** Flink parses micros and nanos, but the writer's timer is in whole milliseconds; toMillis() would floor silently. */
    private static void requireWholeMillis(String key, Duration value) {
        if (value.compareTo(Duration.ofMillis(1)) < 0 || value.getNano() % 1_000_000 != 0) {
            throw new ValidationException(String.format(
                    "'%s' must be a whole number of milliseconds, at least 1 ms, but was %s.",
                    key, TimeUtils.formatWithHighestUnit(value)));
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
        Map<String, String> clientOptions = clientOptions(tableOptions);
        Map<String, String> serverSettings = serverSettings(tableOptions);
        checkServerSettingDefinedOnce(clientOptions, serverSettings);
        ClickHouseClientConfig clientConfig = new ClickHouseClientConfig(
                options.get(URL),
                options.get(USERNAME),
                options.get(PASSWORD),
                options.get(DATABASE),
                options.get(TABLE),
                clientOptions,
                serverSettings,
                toRetryPolicy(options.get(SINK_MAX_RETRIES)));
        clientConfig.setBatchFailureStrategy(
                parseBatchFailureStrategy(options.get(SINK_BATCH_FAILURE_STRATEGY)));
        return clientConfig;
    }

    /**
     * Reads the table's current column types through a short-lived, pinged client —
     * deliberately unmemoized so a long-lived planner sees {@code ALTER TABLE}.
     */
    static TableSchema introspect(ReadableConfig options, ClickHouseClientConfig clientConfig) {
        String url = options.get(URL);
        String database = options.get(DATABASE);
        String table = options.get(TABLE);
        LOG.info("Introspecting ClickHouse table {}.{} at {}", database, table, url);
        try (Client client = clientConfig.createPlanningClient(PLANNING_SERVER_SETTINGS)) {
            return client.getTableSchema(table, database);
        } catch (IllegalArgumentException | ClientMisconfigurationException e) {
            // Only Client.Builder.build() throws these here: option values and combinations parseConfigMap cannot see (the time-zone pair, SSL authentication, key store and certificate files).
            throw new ValidationException(String.format(
                    "Invalid '%s*' option or option combination — the ClickHouse client rejected it: %s",
                    CLIENT_OPTIONS_PREFIX, withCause(e)), e);
        } catch (Exception e) {
            throw new ValidationException(String.format(
                    "Could not read the schema of ClickHouse table %s.%s at %s — %s",
                    database, table, url, rootMessage(e)), e);
        }
    }

    /** client-v2 wraps a failed DESCRIBE in the constant "Failed to get table schema"; the server's reason is a cause below it, an interrupt's or a timeout's (a message-less TimeoutException) is the wrapper's own message. */
    static String rootMessage(Throwable e) {
        Throwable cause = e;
        while (!(cause instanceof ServerException) && cause.getCause() != null
                && !(cause.getCause() instanceof InterruptedException)
                && cause.getCause().getMessage() != null) {
            cause = cause.getCause();
        }
        return cause.getMessage() != null ? cause.getMessage() : cause.toString();
    }

    /** build()'s SSL failures name the unreadable file or bad password only in a cause. */
    private static String withCause(Throwable e) {
        return e.getCause() == null ? e.getMessage() : e.getMessage() + " (" + rootMessage(e.getCause()) + ")";
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

    static ZoneId parseSinkTimezone(String zone) {
        try {
            // Fixed-offset ids (UTC, GMT, Etc/UTC) become ZoneOffsets: 'Z' in state, no rule lookup per value.
            return ZoneId.of(zone).normalized();
        } catch (DateTimeException e) {
            throw new ValidationException(String.format(
                    "Invalid value '%s' for '%s': %s", zone, SINK_TIMEZONE.key(), e.getMessage()), e);
        }
    }

    /** client-v2 parses the endpoint (java.net.URL, http/https) only when the client is built, inside the introspection step. */
    static void validateUrl(String url) {
        String problem = urlProblem(url);
        if (problem != null) {
            throw new ValidationException(String.format(
                    "Invalid value '%s' for '%s': %s — expected an endpoint like 'http://host:8123'.",
                    url, URL.key(), problem));
        }
    }

    /** The client's own endpoint checks (URL syntax, http/https, host, port range) on a throwaway builder; no I/O. */
    private static String urlProblem(String url) {
        try {
            new Client.Builder().addEndpoint(url);
            return null;
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        }
    }

    static RetryPolicy toRetryPolicy(int maxRetries) {
        if (maxRetries == MAX_RETRIES_UNLIMITED) {
            return RetryPolicy.forever();
        }
        if (maxRetries < 0) {
            throw new ValidationException(String.format(
                    "'%s' must be >= 0, or -1 for unlimited retries, but was %d.",
                    SINK_MAX_RETRIES.key(), maxRetries));
        }
        return RetryPolicy.limited(maxRetries);
    }

    static BatchFailureStrategy parseBatchFailureStrategy(String value) {
        try {
            return BatchFailureStrategy.valueOf(
                    value.trim().toUpperCase(Locale.ROOT).replace('-', '_'));
        } catch (IllegalArgumentException e) {
            throw new ValidationException(String.format(
                    "Invalid value '%s' for '%s' — supported values: 'stop-flink', 'drop-batch'.",
                    value, SINK_BATCH_FAILURE_STRATEGY.key()));
        }
    }

    /** The client-v2 passthrough; the client only WARN-logs unknown keys, so they are rejected here. */
    static Map<String, String> clientOptions(Map<String, String> tableOptions) {
        Map<String, String> options = prefixedOptions(tableOptions, CLIENT_OPTIONS_PREFIX);
        options.keySet().forEach(ClickHouseDynamicTableSinkFactory::checkClientOptionKey);
        options.forEach(ClickHouseDynamicTableSinkFactory::checkClientOptionValue);
        return options;
    }

    /** The clickhouse.server.* passthrough; the connector's own settings are rejected rather than fought over. */
    static Map<String, String> serverSettings(Map<String, String> tableOptions) {
        Map<String, String> settings = prefixedOptions(tableOptions, SERVER_SETTINGS_PREFIX);
        settings.keySet().forEach(setting ->
                checkServerSettingNotConnectorOwned(SERVER_SETTINGS_PREFIX + setting, setting));
        return settings;
    }

    /** Both key forms land on one client-builder key, the server one last, so the pair would collapse silently. */
    static void checkServerSettingDefinedOnce(Map<String, String> clientOptions, Map<String, String> serverSettings) {
        String prefix = ClientConfigProperties.SERVER_SETTING_PREFIX;
        for (String key : clientOptions.keySet()) {
            if (key.startsWith(prefix) && serverSettings.containsKey(key.substring(prefix.length()))) {
                throw new ValidationException(String.format(
                        "Option '%s%s' duplicates '%s%s' — set the server setting once.",
                        CLIENT_OPTIONS_PREFIX, key, SERVER_SETTINGS_PREFIX, key.substring(prefix.length())));
            }
        }
    }

    private static void checkClientOptionKey(String key) {
        checkNotConnectionOption(CLIENT_OPTIONS_PREFIX + key, RESERVED_CLIENT_KEYS.get(key));
        // client-v2 strips the prefix and sends the rest verbatim, so a bare one becomes an empty setting or header name.
        if (key.equals(ClientConfigProperties.HTTP_HEADER_PREFIX) || key.equals(ClientConfigProperties.SERVER_SETTING_PREFIX)) {
            throw new ValidationException(String.format(
                    "Option '%s%s' has no name after the prefix — expected '%s%s<name>'.",
                    CLIENT_OPTIONS_PREFIX, key, CLIENT_OPTIONS_PREFIX, key));
        }
        if (key.startsWith(ClientConfigProperties.SERVER_SETTING_PREFIX)) {
            checkServerSettingNotConnectorOwned(CLIENT_OPTIONS_PREFIX + key,
                    key.substring(ClientConfigProperties.SERVER_SETTING_PREFIX.length()));
        }
        if (key.startsWith(ClientConfigProperties.HTTP_HEADER_PREFIX)) {
            checkNotConnectionOption(CLIENT_OPTIONS_PREFIX + key, RESERVED_CLIENT_HEADERS.get(
                    key.substring(ClientConfigProperties.HTTP_HEADER_PREFIX.length()).toLowerCase(Locale.ROOT)));
        }
        if (!isClientOptionKey(key)) {
            throw new ValidationException(String.format(
                    "Option '%s%s' is not a ClickHouse client option. Supported keys: %s; "
                    + "'%s<name>' and '%s<name>' are accepted too.",
                    CLIENT_OPTIONS_PREFIX, key, supportedClientKeys(),
                    ClientConfigProperties.HTTP_HEADER_PREFIX, ClientConfigProperties.SERVER_SETTING_PREFIX));
        }
    }

    /** Client.Builder.build() parses option values eagerly, which would report a typo as a schema failure. */
    private static void checkClientOptionValue(String key, String value) {
        try {
            ClientConfigProperties.parseConfigMap(Map.of(key, value));
        } catch (RuntimeException e) {
            throw new ValidationException(String.format(
                    "Invalid value '%s' for '%s%s': %s", value, CLIENT_OPTIONS_PREFIX, key, e.getMessage()), e);
        }
    }

    /** The client sends these as its own headers; as a server setting the server ignores (database) or rejects (user, password) the copy without naming the option. */
    private static void checkNotConnectionOption(String optionKey, ConfigOption<?> firstClass) {
        if (firstClass != null) {
            throw new ValidationException(String.format(
                    "Option '%s' duplicates the connection option '%s' — set '%s' instead.",
                    optionKey, firstClass.key(), firstClass.key()));
        }
    }

    /** Whichever request carries the connector's copy, the client keeps exactly one value per setting, so the user's would lose silently. */
    private static void checkServerSettingNotConnectorOwned(String optionKey, String setting) {
        checkNotConnectionOption(optionKey, RESERVED_CLIENT_KEYS.get(setting));
        if (PRINT_PRETTY_TYPE_NAMES.equals(setting)) {
            throw connectorOwnedSetting(optionKey, "with its schema introspection");
        }
        if (INSERT_SERVER_SETTINGS.contains(setting)) {
            throw connectorOwnedSetting(optionKey, "with every insert");
        }
    }

    private static ValidationException connectorOwnedSetting(String optionKey, String sentWith) {
        return new ValidationException(String.format(
                "Option '%s' collides with the setting the connector itself sends %s — remove it.",
                optionKey, sentWith));
    }

    private static boolean isClientOptionKey(String key) {
        return key.startsWith(ClientConfigProperties.HTTP_HEADER_PREFIX)
                || key.startsWith(ClientConfigProperties.SERVER_SETTING_PREFIX)
                || Arrays.stream(ClientConfigProperties.values()).anyMatch(p -> p.getKey().equals(key));
    }

    private static String supportedClientKeys() {
        return Arrays.stream(ClientConfigProperties.values())
                .map(ClientConfigProperties::getKey)
                .filter(key -> !RESERVED_CLIENT_KEYS.containsKey(key))
                .sorted()
                .collect(Collectors.joining(", "));
    }

    /**
     * Collects {@code <prefix><key> = value} table options into a trimmed {@code key -> value} map. A
     * bare prefix is rejected: it would become the empty key, which the client and server silently ignore.
     */
    static Map<String, String> prefixedOptions(Map<String, String> tableOptions, String prefix) {
        Map<String, String> extracted = new HashMap<>();
        tableOptions.forEach((key, value) -> {
            if (key.startsWith(prefix)) {
                String setting = key.substring(prefix.length()).trim();
                if (setting.isEmpty()) {
                    throw new ValidationException(String.format(
                            "Option '%s' has no key after the prefix — expected '%s<key>'.", key, prefix));
                }
                if (extracted.put(setting, value) != null) {
                    throw new ValidationException(String.format(
                            "Option '%s' duplicates another '%s%s' key once whitespace is trimmed — set it once.",
                            key, prefix, setting));
                }
            }
        });
        return extracted;
    }
}
