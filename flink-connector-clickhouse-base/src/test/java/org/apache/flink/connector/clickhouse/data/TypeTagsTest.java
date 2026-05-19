package org.apache.flink.connector.clickhouse.data;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TypeTagsTest {

    private static Object roundTrip(Object value) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            TypeTags.write(value, out);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            return TypeTags.read(in);
        }
    }

    @Test void nullRoundTrip() throws IOException { assertNull(roundTrip(null)); }
    @Test void booleanRoundTrip() throws IOException { assertEquals(true, roundTrip(true)); assertEquals(false, roundTrip(false)); }
    @Test void byteRoundTrip() throws IOException { assertEquals((byte) -42, roundTrip((byte) -42)); }
    @Test void shortRoundTrip() throws IOException { assertEquals((short) 1234, roundTrip((short) 1234)); }
    @Test void intRoundTrip() throws IOException { assertEquals(123456789, roundTrip(123456789)); }
    @Test void longRoundTrip() throws IOException { assertEquals(1234567890123L, roundTrip(1234567890123L)); }
    @Test void floatRoundTrip() throws IOException { assertEquals(3.14f, roundTrip(3.14f)); }
    @Test void doubleRoundTrip() throws IOException { assertEquals(2.71828d, roundTrip(2.71828d)); }

    @Test void bigIntegerRoundTrip() throws IOException {
        BigInteger v = new BigInteger("123456789012345678901234567890");
        assertEquals(v, roundTrip(v));
    }

    @Test void bigDecimalRoundTrip() throws IOException {
        BigDecimal v = new BigDecimal("1234567890.12345");
        assertEquals(v, roundTrip(v));
    }

    @Test void stringRoundTrip() throws IOException {
        assertEquals("hello, world 🌍", roundTrip("hello, world 🌍"));
    }

    @Test void bytesRoundTrip() throws IOException {
        byte[] v = {1, 2, 3, -1, -128, 127};
        assertArrayEquals(v, (byte[]) roundTrip(v));
    }

    @Test void uuidRoundTrip() throws IOException {
        UUID v = UUID.fromString("11111111-2222-3333-4444-555555555555");
        assertEquals(v, roundTrip(v));
    }

    @Test void localDateRoundTrip() throws IOException {
        LocalDate v = LocalDate.of(2026, 5, 16);
        assertEquals(v, roundTrip(v));
    }

    @Test void localDateTimeRoundTrip() throws IOException {
        LocalDateTime v = LocalDateTime.of(2026, 5, 16, 12, 34, 56, 789_000_000);
        assertEquals(v, roundTrip(v));
    }

    @Test void zonedDateTimeRoundTrip() throws IOException {
        ZonedDateTime v = ZonedDateTime.of(2026, 5, 16, 12, 34, 56, 789_000_000, ZoneId.of("Europe/Berlin"));
        assertEquals(v, roundTrip(v));
    }

    @Test void listRoundTrip() throws IOException {
        List<Object> v = Arrays.asList("a", 1, null, true);
        assertEquals(v, roundTrip(v));
    }

    @Test void mapRoundTrip() throws IOException {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("a", 1);
        v.put("b", "two");
        v.put("c", null);
        assertEquals(v, roundTrip(v));
    }

    @Test void tupleRoundTrip() throws IOException {
        Object[] v = {"hello", 42L, true};
        assertArrayEquals(v, (Object[]) roundTrip(v));
    }

    @Test void nestedRoundTrip() throws IOException {
        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("k", Arrays.asList(1, 2, 3));
        Map<String, Object> outer = new LinkedHashMap<>();
        outer.put("nested", inner);
        assertEquals(outer, roundTrip(outer));
    }

    @Test void unsupportedTypeFailsClearly() {
        IOException ex = assertThrows(IOException.class, () -> roundTrip(new java.util.Date()));
        assertTrue(ex.getMessage().contains("Unsupported map value type"));
    }
}
