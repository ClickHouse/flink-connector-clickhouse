package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientMisconfigurationException;
import com.clickhouse.config.BatchFailureStrategy;
import com.clickhouse.config.RetryPolicy;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    /** system.query_log's http_user_agent starts with the name; the api tag tells DataStream jobs from Table API ones. */
    @Test void clientNameAndMetadataIdentifyTheConnectorAndTheApi() {
        ClickHouseClientConfig config = config();
        assertEquals("Flink-ClickHouse-Sink/" + ClickHouseSinkVersion.getVersion(), clientName(config));
        assertEquals(metadata("datastream"), clientMetadata(config));
        config.setApi(ClickHouseClientConfig.Api.TABLE);
        assertEquals(metadata("table"), clientMetadata(config));
    }

    /** The built client only WARNs on unknown keys; a user copy of the ignore key wins. */
    @Test void unknownClientOptionsAreIgnoredUnlessTheUserAsksOtherwise() {
        ClickHouseClientConfig lenient = config();
        lenient.setOptions(Map.of("not_a_client_option", "1"));
        assertDoesNotThrow(() -> lenient.createPlanningClient(Map.of()).close());

        ClickHouseClientConfig strict = config();
        strict.setOptions(Map.of("not_a_client_option", "1", ClientConfigProperties.IGNORE_UNKNOWN_CONFIG_KEY, "false"));
        assertThrows(ClientMisconfigurationException.class, () -> strict.createPlanningClient(Map.of()));
    }

    private static Map<String, String> metadata(String api) {
        return Map.of("fv", "flink/" + EnvironmentInformation.getVersion(), "api", api);
    }

    private static String clientName(ClickHouseClientConfig config) {
        try (Client client = config.createPlanningClient(Map.of())) {
            return client.getConfiguration().get(ClientConfigProperties.CLIENT_NAME.getKey());
        }
    }

    private static Object clientMetadata(ClickHouseClientConfig config) {
        try (Client client = config.createPlanningClient(Map.of())) {
            return ClientConfigProperties.CLIENT_ID_METADATA.parseValue(
                    client.getConfiguration().get(ClientConfigProperties.CLIENT_ID_METADATA.getKey()));
        }
    }
}
