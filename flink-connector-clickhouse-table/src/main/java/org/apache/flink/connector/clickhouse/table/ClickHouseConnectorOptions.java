package org.apache.flink.connector.clickhouse.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

import java.time.Duration;

/**
 * The {@code WITH (...)} options of the {@code 'connector' = 'clickhouse'} SQL sink
 * (docs/table-api/dld-ClickHouseDynamicTableSinkFactory.md).
 *
 * <p>Keys follow the ecosystem convention ({@code sink.buffer-flush.*}, {@code sink.max-retries});
 * defaults are identical to {@code ClickHouseAsyncSinkBuilder}/{@code ClickHouseClientConfig},
 * so SQL and DataStream behave the same. {@code clickhouse.client.<key>} and
 * {@code clickhouse.server.<key>} are prefix-scanned passthroughs, not enumerated here.
 */
public final class ClickHouseConnectorOptions {

    private ClickHouseConnectorOptions() {}

    // ------------------------------------------------------------------------------------
    // Connection (required; password defaults to '')
    // ------------------------------------------------------------------------------------

    public static final ConfigOption<String> URL = ConfigOptions
            .key("url")
            .stringType()
            .noDefaultValue()
            .withDescription("ClickHouse HTTP(S) endpoint, e.g. 'http://localhost:8123'.");

    public static final ConfigOption<String> USERNAME = ConfigOptions
            .key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("ClickHouse user.");

    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("password")
            .stringType()
            .defaultValue("")
            .withDescription("ClickHouse password (masked in logs and EXPLAIN).");

    public static final ConfigOption<String> DATABASE = ConfigOptions
            .key("database")
            .stringType()
            .noDefaultValue()
            .withDescription("Target ClickHouse database.");

    public static final ConfigOption<String> TABLE = ConfigOptions
            .key("table")
            .stringType()
            .noDefaultValue()
            .withDescription("Target ClickHouse table.");

    // ------------------------------------------------------------------------------------
    // Batching / backpressure
    // ------------------------------------------------------------------------------------

    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS = ConfigOptions
            .key("sink.buffer-flush.max-rows")
            .intType()
            .defaultValue(500)
            .withDescription("Rows buffered per insert batch before a flush is triggered.");

    public static final ConfigOption<MemorySize> SINK_BUFFER_FLUSH_MAX_BYTES = ConfigOptions
            .key("sink.buffer-flush.max-bytes")
            .memoryType()
            .defaultValue(MemorySize.parse("5mb"))
            .withDescription("Bytes buffered per insert batch before a flush is triggered.");

    public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL = ConfigOptions
            .key("sink.buffer-flush.interval")
            .durationType()
            .defaultValue(Duration.ofSeconds(5))
            .withDescription("Longest time a record may sit in the buffer before a flush.");

    public static final ConfigOption<Integer> SINK_MAX_IN_FLIGHT_REQUESTS = ConfigOptions
            .key("sink.max-in-flight-requests")
            .intType()
            .defaultValue(50)
            .withDescription("Maximum concurrent in-flight insert requests.");

    public static final ConfigOption<Integer> SINK_MAX_BUFFERED_REQUESTS = ConfigOptions
            .key("sink.max-buffered-requests")
            .intType()
            .defaultValue(10_000)
            .withDescription("Maximum buffered records before backpressure kicks in.");

    public static final ConfigOption<MemorySize> SINK_RECORD_MAX_BYTES = ConfigOptions
            .key("sink.record.max-bytes")
            .memoryType()
            .defaultValue(MemorySize.parse("1mb"))
            .withDescription("Maximum serialized size of a single record.");

    // sink.parallelism is FactoryUtil.SINK_PARALLELISM — validated via the factory helper.

    // ------------------------------------------------------------------------------------
    // Reliability
    // ------------------------------------------------------------------------------------

    public static final ConfigOption<Integer> SINK_MAX_RETRIES = ConfigOptions
            .key("sink.max-retries")
            .intType()
            .defaultValue(-1)
            .withDescription("Retries per failed batch; -1 retries forever.");

    public static final ConfigOption<String> SINK_BATCH_FAILURE_STRATEGY = ConfigOptions
            .key("sink.batch-failure-strategy")
            .stringType()
            .defaultValue("stop-flink")
            .withDescription("What to do when a batch fails non-retriably: 'stop-flink' or 'drop-batch'.");

    // ------------------------------------------------------------------------------------
    // Type / compatibility
    // ------------------------------------------------------------------------------------

    public static final ConfigOption<String> SINK_TIMEZONE = ConfigOptions
            .key("sink.timezone")
            .stringType()
            .defaultValue("UTC")
            .withDescription("Zone in which TIMESTAMP (no time zone) wall-clock values are interpreted.");

    public static final ConfigOption<Boolean> SINK_IGNORE_UNKNOWN_FLINK_COLUMNS = ConfigOptions
            .key("sink.ignore-unknown-flink-columns")
            .booleanType()
            .defaultValue(false)
            .withDescription("Drop Flink columns absent from the ClickHouse table instead of failing.");

    // ------------------------------------------------------------------------------------
    // Passthrough prefixes (prefix-scanned, not enumerated)
    // ------------------------------------------------------------------------------------

    /** {@code clickhouse.client.<key>} → client-v2 {@code ClientConfigProperties} options. */
    public static final String CLIENT_OPTIONS_PREFIX = "clickhouse.client.";

    /** {@code clickhouse.server.<key>} → per-query ClickHouse server settings. */
    public static final String SERVER_SETTINGS_PREFIX = "clickhouse.server.";
}
