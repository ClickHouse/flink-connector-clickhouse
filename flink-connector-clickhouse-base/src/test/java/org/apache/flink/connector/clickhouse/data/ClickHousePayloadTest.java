package org.apache.flink.connector.clickhouse.data;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClickHousePayloadTest {

    @Test void freshPayloadHasEmptyMap() {
        ClickHousePayload p = new ClickHousePayload();
        assertNotNull(p.getData());
        assertTrue(p.getData().isEmpty());
        assertNull(p.getCachedBytes());
        assertFalse(p.isRaw());
        assertTrue(p.needsRehydration());
    }

    @Test void mapIsMutableAfterConstruction() {
        ClickHousePayload p = new ClickHousePayload();
        p.getData().put("k", 42);
        assertEquals(42, p.getData().get("k"));
    }

    @Test void ofRawWrapsBytesUnderReservedKey() {
        byte[] bytes = {1, 2, 3};
        ClickHousePayload p = ClickHousePayload.ofRaw(bytes);
        assertTrue(p.isRaw());
        assertArrayEquals(bytes, (byte[]) p.getData().get(ClickHousePayload.RAW_KEY));
        assertArrayEquals(bytes, p.getCachedBytes());
        assertFalse(p.needsRehydration());
    }

    @Test void restoreConstructorTakesMapBytesNullUntilRehydrated() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("k", "v");
        ClickHousePayload p = new ClickHousePayload(data);
        assertEquals(data, p.getData());
        assertNull(p.getCachedBytes());
        assertTrue(p.needsRehydration());
    }

    @Test void setCachedBytesClearsRehydrationFlag() {
        ClickHousePayload p = new ClickHousePayload(new LinkedHashMap<>());
        p.setCachedBytes(new byte[]{7});
        assertFalse(p.needsRehydration());
    }

    @Test void attemptCountIncrement() {
        ClickHousePayload p = new ClickHousePayload();
        assertEquals(1, p.getAttemptCount());
        p.incrementAttempts();
        p.incrementAttempts();
        assertEquals(3, p.getAttemptCount());
    }

    @Test void rawKeyConstant() {
        assertEquals("__clickhouse_raw__", ClickHousePayload.RAW_KEY);
    }
}
