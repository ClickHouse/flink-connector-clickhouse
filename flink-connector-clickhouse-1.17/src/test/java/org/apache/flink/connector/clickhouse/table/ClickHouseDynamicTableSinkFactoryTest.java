package org.apache.flink.connector.clickhouse.table;

import org.apache.flink.table.api.ValidationException;

import com.clickhouse.config.RetryPolicy;
import org.junit.jupiter.api.Test;

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
}
