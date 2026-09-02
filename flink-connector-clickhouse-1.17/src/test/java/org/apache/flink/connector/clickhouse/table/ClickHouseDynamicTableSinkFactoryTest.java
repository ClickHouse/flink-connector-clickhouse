package org.apache.flink.connector.clickhouse.table;

import org.apache.flink.table.api.ValidationException;

import com.clickhouse.config.BatchFailureStrategy;
import com.clickhouse.config.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Map;

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
}
