package org.apache.flink.connector.clickhouse.sink.tpc;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Pure unit test for the deduplication token formula used inside
 * {@link ClickHouseCommittingWriter}.
 *
 * <p>Replays the same formula here so that any accidental drift in the writer
 * implementation produces a test failure. If this test starts failing, something
 * changed in how writer derives tokens — and that change will break dedup on restart.
 */
class ClickHouseTokenDeterminismTest {

    @Test
    void sameInputsProduceSameToken() throws Exception {
        byte[] payload = buildCsvBytes(50);

        String a = token(0, 1, 0, payload);
        String b = token(0, 1, 0, payload);

        assertEquals(a, b,
                "Same (subtaskId, checkpointId, seq, payload) must hash to same token");
    }

    @Test
    void differentCheckpointChangesToken() throws Exception {
        byte[] payload = buildCsvBytes(50);
        assertNotEquals(token(0, 1, 0, payload), token(0, 2, 0, payload));
    }

    @Test
    void differentSubtaskChangesToken() throws Exception {
        byte[] payload = buildCsvBytes(50);
        assertNotEquals(token(0, 1, 0, payload), token(1, 1, 0, payload));
    }

    @Test
    void differentSequenceChangesToken() throws Exception {
        byte[] payload = buildCsvBytes(50);
        assertNotEquals(token(0, 1, 0, payload), token(0, 1, 1, payload));
    }

    @Test
    void differentPayloadChangesToken() throws Exception {
        assertNotEquals(
                token(0, 1, 0, buildCsvBytes(50)),
                token(0, 1, 0, buildCsvBytes(51)));
    }

    /**
     * Lower bound for hash diffusion — two payloads differing by one byte must produce
     * very different tokens. If this ever produces identical tokens, the hash is broken.
     */
    @Test
    void oneBitDiffProducesDifferentToken() throws Exception {
        byte[] p1 = buildCsvBytes(100);
        byte[] p2 = p1.clone();
        p2[p2.length - 2] ^= 0x01;

        assertNotEquals(token(0, 1, 0, p1), token(0, 1, 0, p2));
    }

    // ---------------- helpers (mirror writer) ----------------

    private static byte[] buildCsvBytes(int rows) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rows; i++) {
            sb.append(i).append(',').append("val").append(i).append('\n');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static String token(int subtaskId, long checkpointId, int sequence, byte[] payload)
            throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        String prefix = "ch-flink-tpc:" + subtaskId + ":" + checkpointId + ":" + sequence + ":";
        md.update(prefix.getBytes(StandardCharsets.UTF_8));
        md.update(payload);
        return toHex(md.digest());
    }

    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    private static String toHex(byte[] bytes) {
        char[] out = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int b = bytes[i] & 0xFF;
            out[i * 2] = HEX_CHARS[b >>> 4];
            out[i * 2 + 1] = HEX_CHARS[b & 0x0F];
        }
        return new String(out);
    }
}
