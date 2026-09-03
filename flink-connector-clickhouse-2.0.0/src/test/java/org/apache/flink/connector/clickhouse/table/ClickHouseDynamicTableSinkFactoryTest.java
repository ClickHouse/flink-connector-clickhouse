package org.apache.flink.connector.clickhouse.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.config.BatchFailureStrategy;
import com.clickhouse.config.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    /** client-v2 only WARN-logs unknown keys, so a typo would be accepted at planning and ignored at runtime. */
    @Test void clientPassthroughRejectsUnknownKeysListingTheSupportedOnes() {
        ValidationException ex = assertThrows(ValidationException.class,
                () -> ClickHouseDynamicTableSinkFactory.clientOptions(Map.of("clickhouse.client.connect_timeout", "1000")));
        assertTrue(ex.getMessage().contains("'clickhouse.client.connect_timeout'"), ex.getMessage());
        assertTrue(ex.getMessage().contains("connection_timeout"), ex.getMessage());
        assertTrue(ex.getMessage().contains("http_header_"), ex.getMessage());
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
        // pingLoop's interrupt wrapper explains what was interrupted; the JDK's "sleep interrupted" does not.
        assertEquals("Interrupted while checking ClickHouse connectivity.", ClickHouseDynamicTableSinkFactory.rootMessage(
                new RuntimeException("Interrupted while checking ClickHouse connectivity.",
                        new InterruptedException("sleep interrupted"))));
    }

    /** client-v2 reports a DESCRIBE timeout as a message-less TimeoutException under a wrapper that names the limit. */
    @Test void introspectionTimeoutKeepsTheWrapperMessage() {
        assertEquals("Operation has likely timed out after 5 seconds.", ClickHouseDynamicTableSinkFactory.rootMessage(
                new ClientException("Operation has likely timed out after 5 seconds.", new TimeoutException())));
    }

    /** client-v2 parses the endpoint only when the client is built, which would report a typo as a schema failure. */
    @Test void urlIsValidatedBeforeAnyNetworkCall() {
        ClickHouseDynamicTableSinkFactory.validateUrl("http://localhost:8123");
        ClickHouseDynamicTableSinkFactory.validateUrl("HTTPS://my_host.example:8443/");
        for (String bad : List.of("localhost:8123", "ftp://host:8123", "http://", "http:/host")) {
            ValidationException ex = assertThrows(ValidationException.class,
                    () -> ClickHouseDynamicTableSinkFactory.validateUrl(bad));
            assertTrue(ex.getMessage().contains("'url'"), ex.getMessage());
            assertTrue(ex.getMessage().contains("'" + bad + "'"), ex.getMessage());
        }
    }

    /** DESCRIBE pretty-prints named Tuples across lines by default; the insert header must carry the canonical name. */
    @Test void serverSettingsForceCanonicalTypeNames() {
        assertEquals(Map.of("async_insert", "1", "print_pretty_type_names", "0"),
                ClickHouseDynamicTableSinkFactory.serverSettings(
                        Map.of("connector", "clickhouse", "clickhouse.server.async_insert", "1")));
    }
}
