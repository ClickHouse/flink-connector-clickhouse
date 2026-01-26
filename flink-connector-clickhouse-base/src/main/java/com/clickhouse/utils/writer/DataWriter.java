package com.clickhouse.utils.writer;

import com.clickhouse.client.api.data_formats.internal.SerializerUtils;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.format.BinaryStreamUtils;
import com.clickhouse.utils.Serialize;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.UUID;

public class DataWriter {
    private boolean defaultsSupport = false;
    private OutputStream out = null;

    private DataWriter(boolean defaultsSupport, OutputStream out) {
        this.out = out;
        this.defaultsSupport = defaultsSupport;
    }

    public static DataWriter of(boolean schemaHasDefaults, OutputStream out) {
        return new DataWriter(schemaHasDefaults, out);
    }

    // Method structure write[ClickHouse Type](OutputStream, Java type, ... )
    // Date support
    public void writeDate(LocalDate value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            SerializerUtils.writeDate(out, value, ZoneId.of("UTC")); // TODO: check
        }
    }

    public void writeDate(ZonedDateTime value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            SerializerUtils.writeDate(out, value, ZoneId.of("UTC")); // TODO: check
        }
    }

    public void writeDate32(LocalDate value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            SerializerUtils.writeDate32(out, value, ZoneId.of("UTC")); // TODO: check
        }
    }

    public void writeDate32(ZonedDateTime value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            SerializerUtils.writeDate32(out, value, ZoneId.of("UTC")); // TODO: check
        }
    }

    // Support for DateTime section
    public void writeTimeDate(LocalDateTime value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            SerializerUtils.writeDateTime(out, value, ZoneId.of("UTC")); // TODO: check
        }
    }

    public void writeTimeDate(ZonedDateTime value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            SerializerUtils.writeDateTime(out, value, ZoneId.of("UTC")); // TODO: check
        }
    }

    public void writeTimeDate64(LocalDateTime value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column, int scale) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            SerializerUtils.writeDateTime64(out, value, scale, ZoneId.of("UTC")); // TODO: check
        }
    }

    public void writeTimeDate64(ZonedDateTime value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column, int scale) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            SerializerUtils.writeDateTime64(out, value, scale, ZoneId.of("UTC")); // TODO: check
        }
    }

    // clickhouse type String support
    public void writeString(String value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeString(out, Serialize.convertToString(value));
        }
    }

    // Add a boundary check before inserting
    public void writeFixedString(String value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column, int size) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeFixedString(out, Serialize.convertToString(value), size);
        }
    }

    // Int8
    public void writeInt8(Byte value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeInt8(out, Serialize.convertToInteger(value));
        }
    }

    // Int16
    public void writeInt16(Short value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeInt16(out, Serialize.convertToInteger(value));
        }
    }

    // Int32
    public void writeInt32(Integer value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeInt32(out, Serialize.convertToInteger(value));
        }
    }

    // Int64
    public void writeInt64(Long value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeInt64(out, Serialize.convertToInteger(value));
        }
    }

    // Int128
    public void writeInt128(BigInteger value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeInt128(out, SerializerUtils.convertToBigInteger(value));
        }
    }

    // Int256
    public void writeInt256(BigInteger value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeInt256(out, SerializerUtils.convertToBigInteger(value));
        }
    }

    public void writeUInt8(int value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeUnsignedInt8(out, value);
        }
    }

    public void writeUInt16(int value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeUnsignedInt16(out, value);
        }
    }

    public void writeUInt32(long value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeUnsignedInt32(out, value);
        }
    }

    public void writeUInt64(long value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeUnsignedInt64(out, value);
        }
    }

    public void writeUInt128(BigInteger value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeUnsignedInt128(out, value);
        }
    }

    public void writeUInt256(BigInteger value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeUnsignedInt256(out, value);
        }
    }
    // Decimal
    public void writeDecimal( BigDecimal value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column, int precision, int scale) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeDecimal(out, value, precision, scale);
        }
    }

    // Float32
    public void writeFloat32(Float value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeFloat32(out, value);
        }
    }

    // Float64
    public void writeFloat64(Double value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeFloat64(out,  value);
        }
    }

    // Boolean
    public void writeBoolean(Boolean value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeBoolean(out, value);
        }
    }

    // UUID
    public void writeUUID(UUID value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeUuid(out, value);
        }
    }

    // Array
    public void writeArray(Object value, ClickHouseColumn column) throws IOException {
        SerializerUtils.serializeArrayData(out, value, column);
    }

    // Map
    public void writeMap(Object value, ClickHouseColumn column) throws IOException {
        SerializerUtils.serializeData(out, value, column);
    }

    // Tuple
    public void writeTuple(Object value, ClickHouseColumn column) throws IOException {
        SerializerUtils.serializeData(out, value, column);
    }

    public void writeJSON(String value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeString(out, Serialize.convertToString(value));
        }
    }

}
