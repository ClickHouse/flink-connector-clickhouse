package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.config.BatchFailureStrategy;
import com.clickhouse.config.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClickHouseClientConfigTest {

    /** The no-ping constructor the Table API factory uses: nothing here touches the network. */
    private static ClickHouseClientConfig config() {
        return new ClickHouseClientConfig("http://localhost:1", "u", "secret", "db", "t",
                Map.of("socket_timeout", "1000"), Map.of("async_insert", "1"), RetryPolicy.limited(2));
    }

    @Test void copyCarriesEveryFieldAndSharesNoMutableState() {
        ClickHouseClientConfig original = config();
        original.setEnableJsonSupportAsString(true);
        original.setBatchFailureStrategy(BatchFailureStrategy.DROP_BATCH);
        original.setSupportDefault(Boolean.TRUE);

        ClickHouseClientConfig copy = original.copy();
        assertNotSame(original, copy);
        assertEquals("t", copy.getTableName());
        assertEquals(RetryPolicy.limited(2), copy.getRetryPolicy());
        assertEquals(BatchFailureStrategy.DROP_BATCH, copy.getBatchFailureStrategy());
        assertTrue(copy.getEnableJsonSupportAsString());
        assertEquals(Boolean.TRUE, copy.getSupportDefault());

        copy.setEnableJsonSupportAsString(false);
        copy.setBatchFailureStrategy(BatchFailureStrategy.STOP_FLINK);
        copy.setRetryPolicy(RetryPolicy.forever());
        copy.setSupportDefault(Boolean.FALSE);

        assertTrue(original.getEnableJsonSupportAsString());
        assertEquals(BatchFailureStrategy.DROP_BATCH, original.getBatchFailureStrategy());
        assertEquals(RetryPolicy.limited(2), original.getRetryPolicy());
        assertEquals(Boolean.TRUE, original.getSupportDefault());
    }
}
