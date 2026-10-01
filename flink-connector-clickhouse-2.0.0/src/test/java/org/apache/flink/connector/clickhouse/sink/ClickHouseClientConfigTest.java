package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.config.BatchFailureStrategy;
import com.clickhouse.config.RetryPolicy;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClickHouseClientConfigTest {

    /** No constructor touches the network, so port 1 is safe here. */
    private static ClickHouseClientConfig config() {
        ClickHouseClientConfig config = new ClickHouseClientConfig("http://localhost:1", "u", "secret", "db", "t",
                Map.of("socket_timeout", "1000"), Map.of("async_insert", "1"), false);
        config.setRetryPolicy(RetryPolicy.limited(2));
        return config;
    }

    @Test void copyCarriesEveryFieldAndSharesNoMutableState() {
        ClickHouseClientConfig original = config();
        original.setEnableJsonSupportAsString(true);
        original.setBatchFailureStrategy(BatchFailureStrategy.DROP_BATCH);
        original.setSupportDefault(Boolean.TRUE);
        original.setApi(ClickHouseClientConfig.Api.TABLE);

        ClickHouseClientConfig copy = original.copy();
        assertNotSame(original, copy);
        assertEquals("t", copy.getTableName());
        assertEquals(RetryPolicy.limited(2), copy.getRetryPolicy());
        assertEquals(BatchFailureStrategy.DROP_BATCH, copy.getBatchFailureStrategy());
        assertTrue(copy.getEnableJsonSupportAsString());
        assertEquals(Boolean.TRUE, copy.getSupportDefault());
        assertEquals(ClickHouseClientConfig.Api.TABLE, copy.getApi());

        copy.setEnableJsonSupportAsString(false);
        copy.setBatchFailureStrategy(BatchFailureStrategy.STOP_FLINK);
        copy.setRetryPolicy(RetryPolicy.forever());
        copy.setSupportDefault(Boolean.FALSE);
        copy.setApi(ClickHouseClientConfig.Api.DATASTREAM);

        assertTrue(original.getEnableJsonSupportAsString());
        assertEquals(BatchFailureStrategy.DROP_BATCH, original.getBatchFailureStrategy());
        assertEquals(RetryPolicy.limited(2), original.getRetryPolicy());
        assertEquals(Boolean.TRUE, original.getSupportDefault());
        assertEquals(ClickHouseClientConfig.Api.TABLE, original.getApi());
    }

    /** system.query_log's http_user_agent starts with this; the tag tells DataStream jobs from Table API ones. */
    @Test void productNameCarriesTheApiTag() {
        ClickHouseClientConfig config = config();
        assertEquals(productName("datastream"), clientName(config));
        config.setApi(ClickHouseClientConfig.Api.TABLE);
        assertEquals(productName("table"), clientName(config));
    }

    private static String productName(String api) {
        return String.format("Flink-ClickHouse-Sink/%s (fv:flink/%s, lv:scala/%s, api:%s)", ClickHouseSinkVersion.getVersion(),
                EnvironmentInformation.getVersion(), EnvironmentInformation.getScalaVersion(), api);
    }

    private static String clientName(ClickHouseClientConfig config) {
        try (Client client = config.createPlanningClient(Map.of())) {
            return client.getConfiguration().get(ClientConfigProperties.CLIENT_NAME.getKey());
        }
    }
}
