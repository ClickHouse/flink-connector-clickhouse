package com.clickhouse.utils.writer;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

class DataWriterDispatchTest {

    private byte[] serialize(Object value, ClickHouseColumn col) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataWriter dw = DataWriter.of(out);
        dw.writeValue(value, col);
        return out.toByteArray();
    }

    @Test void dispatchInt8() throws IOException {
        byte[] r = serialize((byte) 42, ClickHouseColumn.of("c", "Int8"));
        assertTrue(r.length >= 1);
    }

    @Test void dispatchInt32() throws IOException {
        byte[] r = serialize(123456, ClickHouseColumn.of("c", "Int32"));
        assertTrue(r.length >= 4);
    }

    @Test void dispatchInt64() throws IOException {
        byte[] r = serialize(1234567890123L, ClickHouseColumn.of("c", "Int64"));
        assertTrue(r.length >= 8);
    }

    @Test void dispatchUInt8FromInteger() throws IOException {
        byte[] r = serialize(200, ClickHouseColumn.of("c", "UInt8"));
        assertTrue(r.length >= 1);
    }

    @Test void dispatchUInt64FromBigInteger() throws IOException {
        byte[] r = serialize(new BigInteger("18446744073709551615"), ClickHouseColumn.of("c", "UInt64"));
        assertTrue(r.length >= 8);
    }

    @Test void dispatchDecimal() throws IOException {
        byte[] r = serialize(new BigDecimal("123.45"), ClickHouseColumn.of("c", "Decimal(10,2)"));
        assertTrue(r.length > 0);
    }

    @Test void dispatchString() throws IOException {
        byte[] r = serialize("hello", ClickHouseColumn.of("c", "String"));
        assertTrue(r.length > 0);
    }

    @Test void dispatchFixedString() throws IOException {
        byte[] r = serialize("12345", ClickHouseColumn.of("c", "FixedString(5)"));
        assertTrue(r.length >= 5);
    }

    @Test void dispatchUUID() throws IOException {
        byte[] r = serialize(UUID.randomUUID(), ClickHouseColumn.of("c", "UUID"));
        assertTrue(r.length >= 16);
    }

    @Test void dispatchDateFromLocalDate() throws IOException {
        byte[] r = serialize(LocalDate.of(2026, 5, 16), ClickHouseColumn.of("c", "Date"));
        assertTrue(r.length > 0);
    }

    @Test void dispatchDateFromZonedDateTime() throws IOException {
        byte[] r = serialize(ZonedDateTime.now(ZoneId.of("UTC")), ClickHouseColumn.of("c", "Date"));
        assertTrue(r.length > 0);
    }

    @Test void dispatchDateTime64WithScale() throws IOException {
        byte[] r = serialize(LocalDateTime.now(), ClickHouseColumn.of("c", "DateTime64(6)"));
        assertTrue(r.length > 0);
    }

    @Test void dispatchArray() throws IOException {
        ClickHouseColumn col = ClickHouseColumn.of("c", ClickHouseDataType.Array, false,
                ClickHouseColumn.of("", "String"));
        byte[] r = serialize(Arrays.asList("a", "b"), col);
        assertTrue(r.length > 0);
    }

    @Test void dispatchBoolean() throws IOException {
        byte[] r = serialize(true, ClickHouseColumn.of("c", "Bool"));
        assertTrue(r.length >= 1);
    }
}
