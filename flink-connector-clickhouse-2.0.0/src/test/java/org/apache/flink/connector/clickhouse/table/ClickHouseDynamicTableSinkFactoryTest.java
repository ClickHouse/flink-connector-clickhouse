package org.apache.flink.connector.clickhouse.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.table.api.ValidationException;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.config.BatchFailureStrategy;
import com.clickhouse.config.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClickHouseDynamicTableSinkFactoryTest {

    @Test void maxRetriesMinusOneMeansForever() {
        assertEquals(RetryPolicy.forever(), ClickHouseDynamicTableSinkFactory.toRetryPolicy(-1));
    }

    @Test void maxRetriesZeroMeansNoRetries() {
        assertEquals(RetryPolicy.limited(0), ClickHouseDynamicTableSinkFactory.toRetryPolicy(0));
    }

    @Test void maxRetriesMapsVerbatim() {
        assertEquals(RetryPolicy.limited(5), ClickHouseDynamicTableSinkFactory.toRetryPolicy(5));
    }

    /** Only -1 is the documented forever sentinel; other negatives are configuration mistakes. */
    @Test void maxRetriesOtherNegativesAreRejected() {
        ValidationException ex = assertThrows(ValidationException.class,
                () -> ClickHouseDynamicTableSinkFactory.toRetryPolicy(-3));
        assertTrue(ex.getMessage().contains("sink.max-retries"));
        assertTrue(ex.getMessage().contains("-3"));
    }

    @Test void batchFailureStrategyAcceptsTheDocumentedSpellings() {
        assertEquals(BatchFailureStrategy.STOP_FLINK,
                ClickHouseDynamicTableSinkFactory.parseBatchFailureStrategy("stop-flink"));
        assertEquals(BatchFailureStrategy.DROP_BATCH,
                ClickHouseDynamicTableSinkFactory.parseBatchFailureStrategy(" Drop_Batch "));
    }

    @Test void unknownBatchFailureStrategyIsRejectedNamingTheOption() {
        ValidationException ex = assertThrows(ValidationException.class,
                () -> ClickHouseDynamicTableSinkFactory.parseBatchFailureStrategy("retry-forever"));
        assertTrue(ex.getMessage().contains("sink.batch-failure-strategy"));
        assertTrue(ex.getMessage().contains("'retry-forever'"));
    }

    @Test void sinkTimezoneMustBeAValidZoneId() {
        assertEquals(ZoneId.of("Asia/Tokyo"), ClickHouseDynamicTableSinkFactory.parseSinkTimezone("Asia/Tokyo"));
        // The default: a ZoneOffset, so state stores 'Z' and conversion skips the rules lookup.
        assertEquals(ZoneOffset.UTC, ClickHouseDynamicTableSinkFactory.parseSinkTimezone("UTC"));
        ValidationException ex = assertThrows(ValidationException.class,
                () -> ClickHouseDynamicTableSinkFactory.parseSinkTimezone("Mars/Olympus_Mons"));
        assertTrue(ex.getMessage().contains("sink.timezone"));
        assertTrue(ex.getMessage().contains("'Mars/Olympus_Mons'"));
    }

    /** clickhouse.client.* and clickhouse.server.* keys lose their prefix and never mix. */
    @Test void passthroughPrefixesAreStrippedAndKeptApart() {
        Map<String, String> tableOptions = Map.of(
                "connector", "clickhouse",
                "clickhouse.client.socket_timeout", "30000",
                "clickhouse.server.async_insert", "1",
                "clickhouse.server.wait_for_async_insert", "1");
        assertEquals(Map.of("socket_timeout", "30000"),
                ClickHouseDynamicTableSinkFactory.prefixedOptions(
                        tableOptions, ClickHouseConnectorOptions.CLIENT_OPTIONS_PREFIX));
        assertEquals(Map.of("async_insert", "1", "wait_for_async_insert", "1"),
                ClickHouseDynamicTableSinkFactory.prefixedOptions(
                        tableOptions, ClickHouseConnectorOptions.SERVER_SETTINGS_PREFIX));
    }

    /** A bare prefix would otherwise become the empty key, which client-v2 and ClickHouse silently ignore. */
    @Test void barePassthroughPrefixIsRejectedNamingTheOption() {
        for (String prefix : List.of(
                ClickHouseConnectorOptions.CLIENT_OPTIONS_PREFIX, ClickHouseConnectorOptions.SERVER_SETTINGS_PREFIX)) {
            ValidationException ex = assertThrows(ValidationException.class,
                    () -> ClickHouseDynamicTableSinkFactory.prefixedOptions(Map.of(prefix, "1"), prefix));
            assertTrue(ex.getMessage().contains("'" + prefix + "'"));
            assertTrue(ex.getMessage().contains(prefix + "<key>"));
        }
    }

    /** client-v2 strips its own prefixes and sends the rest verbatim, so a bare one would become an empty header or setting name. */
    @Test void bareClientHeaderAndServerSettingPrefixesAreRejectedNamingTheOption() {
        for (String prefix : List.of(ClientConfigProperties.HTTP_HEADER_PREFIX, ClientConfigProperties.SERVER_SETTING_PREFIX)) {
            ValidationException ex = assertThrows(ValidationException.class,
                    () -> ClickHouseDynamicTableSinkFactory.clientOptions(Map.of("clickhouse.client." + prefix, "1")));
            assertTrue(ex.getMessage().contains("'clickhouse.client." + prefix + "'"), ex.getMessage());
            assertTrue(ex.getMessage().contains(prefix + "<name>"), ex.getMessage());
        }
    }

    /** Flink keeps option keys verbatim, so a padded key must not reach the client or server untrimmed. */
    @Test void passthroughKeysAreTrimmed() {
        assertEquals(Map.of("max_insert_block_size", "777777"),
                ClickHouseDynamicTableSinkFactory.prefixedOptions(
                        Map.of("clickhouse.server.max_insert_block_size ", "777777"),
                        ClickHouseConnectorOptions.SERVER_SETTINGS_PREFIX));
    }

    /** Two keys differing only in whitespace would otherwise collapse in HashMap order, not DDL order. */
    @Test void passthroughKeysCollidingAfterTrimAreRejected() {
        ValidationException ex = assertThrows(ValidationException.class,
                () -> ClickHouseDynamicTableSinkFactory.prefixedOptions(
                        Map.of("clickhouse.server.max_insert_block_size", "1000",
                                "clickhouse.server.max_insert_block_size ", "777777"),
                        ClickHouseConnectorOptions.SERVER_SETTINGS_PREFIX));
        assertTrue(ex.getMessage().contains("clickhouse.server.max_insert_block_size"), ex.getMessage());
    }

    /** The AsyncSink re-checks these at task start; failing at planning names the SQL options. */
    @Test void batchingOptionsMustBeMutuallyConsistent() {
        ValidationException rows = assertThrows(ValidationException.class,
                () -> ClickHouseDynamicTableSinkFactory.validateBatchingOptions(
                        Configuration.fromMap(Map.of("sink.buffer-flush.max-rows", "10000"))));
        assertEquals("'sink.max-buffered-requests' (10000) must be strictly greater than "
                + "'sink.buffer-flush.max-rows' (10000).", rows.getMessage());
        ValidationException buffered = assertThrows(ValidationException.class,
                () -> ClickHouseDynamicTableSinkFactory.validateBatchingOptions(
                        Configuration.fromMap(Map.of("sink.max-buffered-requests", "400"))));
        assertEquals("'sink.max-buffered-requests' (400) must be strictly greater than "
                + "'sink.buffer-flush.max-rows' (500).", buffered.getMessage());
        ValidationException bytes = assertThrows(ValidationException.class,
                () -> ClickHouseDynamicTableSinkFactory.validateBatchingOptions(
                        Configuration.fromMap(Map.of("sink.buffer-flush.max-bytes", "512kb"))));
        assertTrue(bytes.getMessage().contains("must be at least 'sink.record.max-bytes'"), bytes.getMessage());
        ClickHouseDynamicTableSinkFactory.validateBatchingOptions(Configuration.fromMap(Map.of()));
    }

    /** Flink parses micros and nanos; a sub-millisecond interval must be rejected, not floored to 0 or 1 ms. */
    @Test void flushIntervalMustBeWholeMilliseconds() {
        for (String bad : List.of("0 ms", "500 micros", "1500 micros")) {
            Configuration options = Configuration.fromMap(Map.of("sink.buffer-flush.interval", bad));
            ValidationException ex = assertThrows(ValidationException.class,
                    () -> ClickHouseDynamicTableSinkFactory.validateBatchingOptions(options));
            assertTrue(ex.getMessage().contains("'sink.buffer-flush.interval'"), ex.getMessage());
            assertTrue(ex.getMessage().contains("whole number of milliseconds"), ex.getMessage());
        }
        ClickHouseDynamicTableSinkFactory.validateBatchingOptions(
                Configuration.fromMap(Map.of("sink.buffer-flush.interval", "2000 micros")));
    }

    /** These keys are set from the first-class options; a passthrough copy would override them silently. */
    @Test void clientPassthroughRejectsKeysOwnedByFirstClassOptions() {
        Map<String, String> firstClass = Map.of("database", "database", "user", "username", "password", "password");
        firstClass.forEach((key, option) -> {
            ValidationException ex = assertThrows(ValidationException.class,
                    () -> ClickHouseDynamicTableSinkFactory.clientOptions(Map.of("clickhouse.client." + key, "x")));
            assertTrue(ex.getMessage().contains("'clickhouse.client." + key + "'"), ex.getMessage());
            assertTrue(ex.getMessage().contains("set '" + option + "' instead"), ex.getMessage());
        });
    }

    /** The client sends database, user and password as its own headers; the header spelling replaces them and the server-setting spelling is ignored or rejected by the server, neither naming the option. */
    @Test void connectionOptionsRespelledAsHeadersOrServerSettingsAreRejected() {
        Map<String, String> settings = Map.of("database", "database", "user", "username", "password", "password");
        settings.forEach((setting, option) -> {
            ValidationException server = assertThrows(ValidationException.class,
                    () -> ClickHouseDynamicTableSinkFactory.serverSettings(Map.of("clickhouse.server." + setting, "x")));
            assertTrue(server.getMessage().contains("'clickhouse.server." + setting + "'"), server.getMessage());
            assertTrue(server.getMessage().contains("set '" + option + "' instead"), server.getMessage());
            ValidationException client = assertThrows(ValidationException.class,
                    () -> ClickHouseDynamicTableSinkFactory.clientOptions(Map.of("clickhouse.client.clickhouse_setting_" + setting, "x")));
            assertTrue(client.getMessage().contains("'clickhouse.client.clickhouse_setting_" + setting + "'"), client.getMessage());
            assertTrue(client.getMessage().contains("set '" + option + "' instead"), client.getMessage());
        });
        // Header names are case-insensitive on the wire, so every casing must be caught.
        Map<String, String> headers = Map.of("X-ClickHouse-Database", "database", "x-clickhouse-user", "username", "X-CLICKHOUSE-KEY", "password");
        headers.forEach((header, option) -> {
            ValidationException ex = assertThrows(ValidationException.class,
                    () -> ClickHouseDynamicTableSinkFactory.clientOptions(Map.of("clickhouse.client.http_header_" + header, "x")));
            assertTrue(ex.getMessage().contains("'clickhouse.client.http_header_" + header + "'"), ex.getMessage());
            assertTrue(ex.getMessage().contains("set '" + option + "' instead"), ex.getMessage());
        });
        assertEquals(Map.of("http_header_X-Trace", "abc", "clickhouse_setting_max_threads", "2"),
                ClickHouseDynamicTableSinkFactory.clientOptions(Map.of(
                        "clickhouse.client.http_header_X-Trace", "abc", "clickhouse.client.clickhouse_setting_max_threads", "2")));
    }

    /** client-v2 only WARN-logs unknown keys, so a typo would be accepted at planning and ignored at runtime. */
    @Test void clientPassthroughRejectsUnknownKeysListingTheSupportedOnes() {
        ValidationException ex = assertThrows(ValidationException.class,
                () -> ClickHouseDynamicTableSinkFactory.clientOptions(Map.of("clickhouse.client.connect_timeout", "1000")));
        assertTrue(ex.getMessage().contains("'clickhouse.client.connect_timeout'"), ex.getMessage());
        assertTrue(ex.getMessage().contains("connection_timeout"), ex.getMessage());
        assertTrue(ex.getMessage().contains("http_header_"), ex.getMessage());
    }

    /** Client.Builder.build() parses values eagerly, which would report a typo as a schema failure. */
    @Test void clientPassthroughRejectsUnparsableValuesNamingTheOption() {
        for (String key : List.of("connection_timeout", "retry")) {
            ValidationException ex = assertThrows(ValidationException.class,
                    () -> ClickHouseDynamicTableSinkFactory.clientOptions(Map.of("clickhouse.client." + key, "abc")));
            assertTrue(ex.getMessage().contains("'clickhouse.client." + key + "'"), ex.getMessage());
            assertTrue(ex.getMessage().contains("'abc'"), ex.getMessage());
        }
    }

    @Test void clientPassthroughAcceptsClientKeysHeadersAndServerSettings() {
        Map<String, String> tableOptions = Map.of(
                "connector", "clickhouse",
                "clickhouse.client.connection_timeout", "1000",
                "clickhouse.client.http_header_X-Trace", "abc",
                "clickhouse.client.clickhouse_setting_max_threads", "2",
                "clickhouse.server.async_insert", "1");
        assertEquals(
                Map.of("connection_timeout", "1000", "http_header_X-Trace", "abc", "clickhouse_setting_max_threads", "2"),
                ClickHouseDynamicTableSinkFactory.clientOptions(tableOptions));
    }

    /** client-v2 wraps every failed DESCRIBE in a constant message; the server's reason must surface. */
    @Test void introspectionErrorsSurfaceTheServerReason() {
        ServerException server = new ServerException(60,
                "Code: 60. DB::Exception: Table db.evnts does not exist. (UNKNOWN_TABLE)");
        assertEquals(server.getMessage(), ClickHouseDynamicTableSinkFactory.rootMessage(
                new ClientException("Failed to get table schema", server)));
        assertEquals("Connection refused", ClickHouseDynamicTableSinkFactory.rootMessage(
                new ClientException("Failed to get table schema",
                        new ClientException("Failed to connect", new ConnectException("Connection refused")))));
        assertEquals("plain", ClickHouseDynamicTableSinkFactory.rootMessage(new RuntimeException("plain")));
        // The wrapper says what was interrupted; the JDK's "sleep interrupted" does not.
        assertEquals("Failed to get table schema", ClickHouseDynamicTableSinkFactory.rootMessage(
                new ClientException("Failed to get table schema", new InterruptedException("sleep interrupted"))));
    }

    /** client-v2 reports a DESCRIBE timeout as a message-less TimeoutException under a wrapper that names the limit. */
    @Test void introspectionTimeoutKeepsTheWrapperMessage() {
        assertEquals("Operation has likely timed out after 5 seconds.", ClickHouseDynamicTableSinkFactory.rootMessage(
                new ClientException("Operation has likely timed out after 5 seconds.", new TimeoutException())));
    }

    /** client-v2 parses the endpoint only when the client is built, which would report a typo or a missing port as a schema failure. */
    @Test void urlIsValidatedBeforeAnyNetworkCall() {
        ClickHouseDynamicTableSinkFactory.validateUrl("http://localhost:8123");
        ClickHouseDynamicTableSinkFactory.validateUrl("HTTPS://my_host.example:8443/");
        for (String bad : List.of("localhost:8123", "ftp://host:8123", "http://", "http:/host",
                "http://localhost", "http://host:99999", "http://:8123")) {
            ValidationException ex = assertThrows(ValidationException.class,
                    () -> ClickHouseDynamicTableSinkFactory.validateUrl(bad));
            assertTrue(ex.getMessage().contains("'url'"), ex.getMessage());
            assertTrue(ex.getMessage().contains("'" + bad + "'"), ex.getMessage());
        }
    }

    /** The planning DESCRIBE needs canonical type names; a user copy of that setting in either spelling would fight it. */
    @Test void printPrettyTypeNamesIsReservedForPlanning() {
        assertEquals(Map.of("async_insert", "1"),
                ClickHouseDynamicTableSinkFactory.serverSettings(
                        Map.of("connector", "clickhouse", "clickhouse.server.async_insert", "1")));
        ValidationException server = assertThrows(ValidationException.class,
                () -> ClickHouseDynamicTableSinkFactory.serverSettings(
                        Map.of("clickhouse.server.print_pretty_type_names", "1")));
        assertTrue(server.getMessage().contains("'clickhouse.server.print_pretty_type_names'"), server.getMessage());
        ValidationException client = assertThrows(ValidationException.class,
                () -> ClickHouseDynamicTableSinkFactory.clientOptions(
                        Map.of("clickhouse.client.clickhouse_setting_print_pretty_type_names", "1")));
        assertTrue(client.getMessage().contains("'clickhouse.client.clickhouse_setting_print_pretty_type_names'"),
                client.getMessage());
    }

    /** Client.Builder.build() checks option combinations parseConfigMap cannot see; they must read as option errors, not schema failures. */
    @Test void clientOptionCombinationsTheBuilderRejectsAreReportedAsOptionErrors() {
        // The key and value pass the per-option pre-validation on their own …
        assertEquals(Map.of("use_time_zone", "Asia/Tokyo"),
                ClickHouseDynamicTableSinkFactory.clientOptions(Map.of("clickhouse.client.use_time_zone", "Asia/Tokyo")));
        // … and only Client.Builder.build() rejects them against the default use_server_time_zone=true, before any I/O.
        ValidationException ex = assertThrows(ValidationException.class, () -> ClickHouseDynamicTableSinkFactory.introspect(
                planningOptions("http://localhost:1"), planningConfig("http://localhost:1", Map.of("use_time_zone", "Asia/Tokyo"))));
        assertTrue(ex.getMessage().contains("'clickhouse.client.*'"), ex.getMessage());
        assertTrue(ex.getMessage().contains("USE_TIME_ZONE"), ex.getMessage());
        assertFalse(ex.getMessage().contains("Could not read the schema"), ex.getMessage());
    }

    /** Single values only the built client rejects: a TimeZone-typed value parseConfigMap turns into GMT, and an SSL file the client cannot open (a ClientMisconfigurationException, not an IllegalArgumentException). */
    @Test void clientOptionValuesOnlyTheBuiltClientRejectsAreReportedAsOptionErrors() {
        assertEquals(Map.of("server_time_zone", "Mars/Olympus_Mons"),
                ClickHouseDynamicTableSinkFactory.clientOptions(Map.of("clickhouse.client.server_time_zone", "Mars/Olympus_Mons")));
        ValidationException zone = assertThrows(ValidationException.class, () -> ClickHouseDynamicTableSinkFactory.introspect(
                planningOptions("http://localhost:1"), planningConfig("http://localhost:1", Map.of("server_time_zone", "Mars/Olympus_Mons"))));
        assertTrue(zone.getMessage().contains("'clickhouse.client.*'"), zone.getMessage());
        assertTrue(zone.getMessage().contains("Mars/Olympus_Mons"), zone.getMessage());
        assertFalse(zone.getMessage().contains("Could not read the schema"), zone.getMessage());

        // An https endpoint builds the SSL context inside build(), before any request is made.
        ValidationException ssl = assertThrows(ValidationException.class, () -> ClickHouseDynamicTableSinkFactory.introspect(
                planningOptions("https://localhost:1"), planningConfig("https://localhost:1", Map.of("sslrootcert", "/nonexistent/ca.pem"))));
        assertTrue(ssl.getMessage().contains("'clickhouse.client.*'"), ssl.getMessage());
        assertTrue(ssl.getMessage().contains("SSL context"), ssl.getMessage());
        assertTrue(ssl.getMessage().contains("/nonexistent/ca.pem"), ssl.getMessage());
        assertFalse(ssl.getMessage().contains("Could not read the schema"), ssl.getMessage());
    }

    /** ClickHouseAsyncWriter pins these on every insert and client-v2 lets operation settings win, so a user copy would be discarded silently. */
    @Test void serverSettingsTheWriterPinsPerInsertAreRejectedInEitherSpelling() {
        for (String setting : ClickHouseDynamicTableSinkFactory.INSERT_SERVER_SETTINGS) {
            ValidationException server = assertThrows(ValidationException.class,
                    () -> ClickHouseDynamicTableSinkFactory.serverSettings(Map.of("clickhouse.server." + setting, "0")));
            assertTrue(server.getMessage().contains("'clickhouse.server." + setting + "'"), server.getMessage());
            assertTrue(server.getMessage().contains("every insert"), server.getMessage());
            ValidationException client = assertThrows(ValidationException.class,
                    () -> ClickHouseDynamicTableSinkFactory.clientOptions(
                            Map.of("clickhouse.client.clickhouse_setting_" + setting, "0")));
            assertTrue(client.getMessage().contains("'clickhouse.client.clickhouse_setting_" + setting + "'"), client.getMessage());
        }
        assertEquals(List.of("input_format_binary_read_json_as_string", "input_format_defaults_for_omitted_fields",
                        "input_format_null_as_default"),
                ClickHouseDynamicTableSinkFactory.INSERT_SERVER_SETTINGS.stream().sorted().collect(java.util.stream.Collectors.toList()));
    }

    /**
     * ClickHouseAsyncWriter pins ASYNC_OPERATIONS on its InsertSettings too, and client-v2 lets
     * operation settings win. A client-level copy would govern the planning ping and DESCRIBE
     * while every insert stayed async — the silent half-effect this guard exists to reject.
     */
    @Test void clientOptionsTheWriterPinsPerInsertAreRejected() {
        for (String option : ClickHouseDynamicTableSinkFactory.INSERT_CLIENT_OPTIONS) {
            ValidationException e = assertThrows(ValidationException.class,
                    () -> ClickHouseDynamicTableSinkFactory.clientOptions(
                            Map.of("clickhouse.client." + option, "false")));
            assertTrue(e.getMessage().contains("'clickhouse.client." + option + "'"), e.getMessage());
            assertTrue(e.getMessage().contains("every insert"), e.getMessage());
            // And it is not advertised as settable by the unknown-key error either.
            ValidationException unknown = assertThrows(ValidationException.class,
                    () -> ClickHouseDynamicTableSinkFactory.clientOptions(
                            Map.of("clickhouse.client.not_an_option", "1")));
            assertFalse(unknown.getMessage().contains(option + ","), unknown.getMessage());
        }
        assertEquals(List.of("async"), List.copyOf(ClickHouseDynamicTableSinkFactory.INSERT_CLIENT_OPTIONS));
    }

    private static Configuration planningOptions(String url) {
        return Configuration.fromMap(Map.of("url", url, "database", "db", "table", "t"));
    }

    /** Port 1: nothing here reaches the network. */
    private static ClickHouseClientConfig planningConfig(String url, Map<String, String> clientOptions) {
        return new ClickHouseClientConfig(url, "u", "", "db", "t", clientOptions, Map.of(), false);
    }

    /** clickhouse.server.<k> and clickhouse.client.clickhouse_setting_<k> land on one builder key; the pair must not collapse silently. */
    @Test void serverSettingDefinedThroughBothPrefixesIsRejected() {
        ValidationException ex = assertThrows(ValidationException.class,
                () -> ClickHouseDynamicTableSinkFactory.checkServerSettingDefinedOnce(
                        Map.of("clickhouse_setting_async_insert", "0"), Map.of("async_insert", "1")));
        assertTrue(ex.getMessage().contains("'clickhouse.client.clickhouse_setting_async_insert'"), ex.getMessage());
        assertTrue(ex.getMessage().contains("'clickhouse.server.async_insert'"), ex.getMessage());
        ClickHouseDynamicTableSinkFactory.checkServerSettingDefinedOnce(
                Map.of("clickhouse_setting_max_threads", "2", "socket_timeout", "1000"), Map.of("async_insert", "1"));
    }
}
