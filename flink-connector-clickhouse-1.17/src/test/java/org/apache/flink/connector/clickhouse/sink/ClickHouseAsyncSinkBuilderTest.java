package org.apache.flink.connector.clickhouse.sink;

import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClickHouseAsyncSinkBuilderTest {

    /** Port 1 refuses at once, so the default ping fails after its fixed three attempts. */
    private static ClickHouseAsyncSinkBuilder<String> unreachable() {
        return ClickHouseAsyncSink.<String>builder()
                .setElementConverter(new ClickHouseConvertor<>(String.class))
                .setClickHouseClientConfig(new ClickHouseClientConfig("http://localhost:1", "u", "", "db", "t"));
    }

    @Test void buildFailsFastWhenTheServerIsUnreachable() {
        RuntimeException e = assertThrows(RuntimeException.class, () -> unreachable().build());
        assertTrue(e.getMessage().contains("not accessible"), e.getMessage());
    }

    @Test void buildCanSkipTheConnectivityCheck() {
        assertNotNull(unreachable().setVerifyConnectivity(false).build());
    }
}
