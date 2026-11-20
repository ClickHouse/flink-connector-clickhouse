package org.apache.flink.connector.clickhouse.sink.tpc;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Round-trip tests for {@link ClickHouseCommittableSerializer}. Committables travel
 * through Flink's checkpoint state; serialization must be lossless and backward-compatible.
 */
class ClickHouseCommittableSerializerTest {

    private final ClickHouseCommittableSerializer serializer = new ClickHouseCommittableSerializer();

    @Test
    void roundTrip_preservesAllFields() throws IOException {
        byte[] payload = "row1\nrow2\nrow3".getBytes(StandardCharsets.UTF_8);
        ClickHouseCommittable original = new ClickHouseCommittable(
                payload,
                "my_table",
                "RowBinary",
                "sha256:deadbeef",
                3);

        byte[] bytes = serializer.serialize(original);
        ClickHouseCommittable restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertArrayEquals(payload, restored.getPayload());
        assertEquals("my_table", restored.getTableName());
        assertEquals("RowBinary", restored.getClickHouseFormat());
        assertEquals("sha256:deadbeef", restored.getDeduplicationToken());
        assertEquals(3, restored.getRecordCount());
        assertEquals(payload.length, restored.getPayloadSize());
    }

    @Test
    void roundTrip_handlesEmptyPayload() throws IOException {
        ClickHouseCommittable empty = new ClickHouseCommittable(
                new byte[0], "t", "RowBinary", "tok", 0);

        byte[] bytes = serializer.serialize(empty);
        ClickHouseCommittable restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(0, restored.getPayload().length);
        assertEquals(0, restored.getRecordCount());
    }

    @Test
    void roundTrip_handlesLargePayload() throws IOException {
        byte[] payload = new byte[10 * 1024 * 1024];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i & 0xFF);

        ClickHouseCommittable big = new ClickHouseCommittable(
                payload, "t", "RowBinary", "tok", 100_000);

        byte[] bytes = serializer.serialize(big);
        ClickHouseCommittable restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertArrayEquals(payload, restored.getPayload());
    }

    @Test
    void deserialize_unsupportedVersionThrows() {
        assertThrows(IOException.class,
                () -> serializer.deserialize(99, new byte[] {0, 1, 2, 3}));
    }

    @Test
    void version_isPositive() {
        // Version must be stable across releases to preserve savepoint compatibility.
        assertEquals(1, serializer.getVersion());
    }
}
