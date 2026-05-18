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
    private final OutputStream out;

    private DataWriter(OutputStream out) {
        this.out = out;
    }

    public static DataWriter of(OutputStream out) {
        return new DataWriter(out);
    }

    // Method structure write[ClickHouse Type](OutputStream, Java type, ... )
    // Date support
    public void writeDate(LocalDate value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            SerializerUtils.writeDate(out, value, ZoneId.of("UTC")); // TODO: check
        }
    }

    public void writeDate(ZonedDateTime value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            SerializerUtils.writeDate(out, value, ZoneId.of("UTC")); // TODO: check
        }
    }

    public void writeDate32(LocalDate value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            SerializerUtils.writeDate32(out, value, ZoneId.of("UTC")); // TODO: check
        }
    }

    public void writeDate32(ZonedDateTime value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            SerializerUtils.writeDate32(out, value, ZoneId.of("UTC")); // TODO: check
        }
    }

    // Support for DateTime section
    public void writeDateTime(LocalDateTime value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            SerializerUtils.writeDateTime(out, value, ZoneId.of("UTC")); // TODO: check
        }
    }

    public void writeDateTime(ZonedDateTime value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            SerializerUtils.writeDateTime(out, value, ZoneId.of("UTC")); // TODO: check
        }
    }

    public void writeDateTime64(LocalDateTime value, boolean isNullable, ClickHouseDataType dataType, String column, int scale) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            SerializerUtils.writeDateTime64(out, value, scale, ZoneId.of("UTC")); // TODO: check
        }
    }

    public void writeDateTime64(ZonedDateTime value, boolean isNullable, ClickHouseDataType dataType, String column, int scale) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            SerializerUtils.writeDateTime64(out, value, scale, ZoneId.of("UTC")); // TODO: check
        }
    }

    // clickhouse type String support
    public void writeString(String value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            BinaryStreamUtils.writeString(out, Serialize.convertToString(value));
        }
    }

    // Add a boundary check before inserting
    public void writeFixedString(String value, boolean isNullable, ClickHouseDataType dataType, String column, int size) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            BinaryStreamUtils.writeFixedString(out, Serialize.convertToString(value), size);
        }
    }

    // Int8
    public void writeInt8(Byte value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            BinaryStreamUtils.writeInt8(out, value);
        }
    }

    // Int16
    public void writeInt16(Short value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            BinaryStreamUtils.writeInt16(out, value);
        }
    }

    // Int32
    public void writeInt32(Integer value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            BinaryStreamUtils.writeInt32(out, value);
        }
    }

    // Int64
    public void writeInt64(Long value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            BinaryStreamUtils.writeInt64(out, value);
        }
    }

    // Int128
    public void writeInt128(BigInteger value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            BinaryStreamUtils.writeInt128(out, SerializerUtils.convertToBigInteger(value));
        }
    }

    // Int256
    public void writeInt256(BigInteger value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            BinaryStreamUtils.writeInt256(out, SerializerUtils.convertToBigInteger(value));
        }
    }

    public void writeUInt8(int value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            BinaryStreamUtils.writeUnsignedInt8(out, value);
        }
    }

    public void writeUInt16(int value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            BinaryStreamUtils.writeUnsignedInt16(out, value);
        }
    }

    public void writeUInt32(long value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            BinaryStreamUtils.writeUnsignedInt32(out, value);
        }
    }

    public void writeUInt64(BigInteger value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            BinaryStreamUtils.writeUnsignedInt64(out, value);
        }
    }

    public void writeUInt64(long value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            BinaryStreamUtils.writeUnsignedInt64(out, value);
        }
    }

    public void writeUInt128(BigInteger value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            BinaryStreamUtils.writeUnsignedInt128(out, value);
        }
    }

    public void writeUInt256(BigInteger value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            BinaryStreamUtils.writeUnsignedInt256(out, value);
        }
    }
    // Decimal
    public void writeDecimal(BigDecimal value, boolean isNullable, ClickHouseDataType dataType, String column, int precision, int scale) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            BinaryStreamUtils.writeDecimal(out, value, precision, scale);
        }
    }

    // Float32
    public void writeFloat32(Float value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            BinaryStreamUtils.writeFloat32(out, value);
        }
    }

    // Float64
    public void writeFloat64(Double value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            BinaryStreamUtils.writeFloat64(out,  value);
        }
    }

    // Boolean
    public void writeBoolean(Boolean value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            BinaryStreamUtils.writeBoolean(out, value);
        }
    }

    // UUID
    public void writeUUID(UUID value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
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

    public void writeJSON(String value, boolean isNullable, ClickHouseDataType dataType, String column) throws IOException {
        if (Serialize.writeValuePreamble(out, isNullable, dataType, column, value)) {
            BinaryStreamUtils.writeString(out, Serialize.convertToString(value));
        }
    }

    /**
     * Generic dispatcher: routes a Map value to the right typed writer based on
     * {@code column.getDataType()}. Per design spec §8a.
     */
    public void writeValue(Object value, ClickHouseColumn column) throws IOException {
        ClickHouseDataType type = column.getDataType();
        boolean nullable = column.isNullable();
        boolean lowCard  = column.isLowCardinality();
        String  name     = column.getColumnName();

        switch (type) {
            case Int8:    writeInt8   ((Byte)       value, nullable, type, name); break;
            case Int16:   writeInt16  ((Short)      value, nullable, type, name); break;
            case Int32:   writeInt32  ((Integer)    value, nullable, type, name); break;
            case Int64:   writeInt64  ((Long)       value, nullable, type, name); break;
            case Int128:  writeInt128 ((BigInteger) value, nullable, type, name); break;
            case Int256:  writeInt256 ((BigInteger) value, nullable, type, name); break;
            case UInt8:   writeUInt8  (value == null ? 0 : ((Number) value).intValue(),  nullable, type, name); break;
            case UInt16:  writeUInt16 (value == null ? 0 : ((Number) value).intValue(),  nullable, type, name); break;
            case UInt32:  writeUInt32 (value == null ? 0L : ((Number) value).longValue(), nullable, type, name); break;
            case UInt64:
                if (value instanceof BigInteger) {
                    writeUInt64((BigInteger) value, nullable, type, name);
                } else {
                    writeUInt64(((Long) value).longValue(), nullable, type, name);
                }
                break;
            case UInt128: writeUInt128((BigInteger) value, nullable, type, name); break;
            case UInt256: writeUInt256((BigInteger) value, nullable, type, name); break;
            case Float32: writeFloat32((Float)  value, nullable, type, name); break;
            case Float64: writeFloat64((Double) value, nullable, type, name); break;
            case Bool:    writeBoolean((Boolean) value, nullable, type, name); break;
            case Decimal: case Decimal32: case Decimal64: case Decimal128: case Decimal256:
                writeDecimal((BigDecimal) value, nullable, type, name,
                             column.getPrecision(), column.getScale());
                break;
            case String:      writeString     ((java.lang.String) value, nullable, type, name); break;
            case FixedString: writeFixedString((java.lang.String) value, nullable, type, name,
                                               column.getEstimatedLength()); break;
            case JSON:        writeJSON       ((java.lang.String) value, nullable, type, name); break;
            case UUID:        writeUUID       ((UUID) value, nullable, type, name); break;
            case Date:
                if (value instanceof LocalDate) {
                    writeDate((LocalDate) value, nullable, type, name);
                } else {
                    writeDate((ZonedDateTime) value, nullable, type, name);
                }
                break;
            case Date32:
                if (value instanceof LocalDate) {
                    writeDate32((LocalDate) value, nullable, type, name);
                } else {
                    writeDate32((ZonedDateTime) value, nullable, type, name);
                }
                break;
            case DateTime:
                if (value instanceof LocalDateTime) {
                    writeDateTime((LocalDateTime) value, nullable, type, name);
                } else {
                    writeDateTime((ZonedDateTime) value, nullable, type, name);
                }
                break;
            case DateTime64:
                if (value instanceof LocalDateTime) {
                    writeDateTime64((LocalDateTime) value, nullable, type, name, column.getScale());
                } else {
                    writeDateTime64((ZonedDateTime) value, nullable, type, name, column.getScale());
                }
                break;
            case Array: writeArray(value, column); break;
            case Map:   writeMap  (value, column); break;
            case Tuple: writeTuple(value, column); break;
            default:    throw new IOException("Unsupported ClickHouseDataType: " + type);
        }
    }

}
