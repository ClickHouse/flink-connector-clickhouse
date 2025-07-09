package com.clickhouse.utils;

import com.clickhouse.client.api.data_formats.internal.SerializerUtils;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.format.BinaryStreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

public class Serialize {

    private static final Logger LOG = LoggerFactory.getLogger(Serialize.class);
    public static boolean writePrimitiveValuePreamble(OutputStream out, boolean defaultsSupport, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        // since it is primitive we always have a value that is not null
        if (defaultsSupport) {
            // Add indicator since the table has default values
            SerializerUtils.writeNonNull(out);
        }
        // if the column is Nullable need to add an indicator for nullable
        if (isNullable) {
            SerializerUtils.writeNonNull(out);
        }
        return true;
    }
    public static boolean writeValuePreamble(OutputStream out, boolean defaultsSupport, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column, Object value) throws IOException {
        LOG.debug("writeValuePreamble[defaultsSupport='%s', isNullable='%s', dataType='%s', column='%s', value='%s']");
        if (defaultsSupport) {
            if (value != null) {
                SerializerUtils.writeNonNull(out);
                if (isNullable) {
                    SerializerUtils.writeNonNull(out);
                }
            } else {
                if (hasDefault) {
                    SerializerUtils.writeNull(out);
                    return false;
                }

                if (isNullable) {
                    SerializerUtils.writeNonNull(out);
                    SerializerUtils.writeNull(out);
                    return false;
                }

                if (dataType == ClickHouseDataType.Array) {
                    SerializerUtils.writeNonNull(out);
                } else if (dataType != ClickHouseDataType.Dynamic) {
                    throw new IllegalArgumentException(String.format("An attempt to write null into not nullable column '%s' of type '%s'", column, dataType));
                }
            }
        } else if (isNullable) {
            if (value == null) {
                SerializerUtils.writeNull(out);
                return false;
            }

            SerializerUtils.writeNonNull(out);
        } else if (value == null) {
            if (dataType == ClickHouseDataType.Array) {
                SerializerUtils.writeNonNull(out);
            } else if (dataType != ClickHouseDataType.Dynamic) {
                throw new IllegalArgumentException(String.format("An attempt to write null into not nullable column '%s' of type '%s'", column, dataType));
            }
        }
        return true;
    }

    public static String convertToString(Object value) {
        return java.lang.String.valueOf(value);
    }

    public static Integer convertToInteger(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        } else if (value instanceof Boolean) {
            return ((Boolean) value) ? 1 : 0;
        } else {
            throw new IllegalArgumentException("Cannot convert object of type " +
                    value.getClass().getName() + " to Integer: " + value);
        }
    }

    public static Map<ClickHouseDataType, Method> mapClickHouseTypeToMethod() {
        Map<ClickHouseDataType, Method> map = new HashMap<>();
        for (Method method : Serialize.class.getMethods()) {
            String name = method.getName();
            if (name.startsWith("write")) {
                String chType = name.substring("write".length());
                try {
                    ClickHouseDataType type = ClickHouseDataType.valueOf(chType);
                    map.put(type, method);
                } catch (IllegalArgumentException e) {
                    System.out.println(e.getMessage());
                }
            }
        }
        return map;
    }

    /**
     *
     */

    // Method structure write[ClickHouse Type](OutputStream, Java type, ... )
    // Date support
    public static void writeDate(OutputStream out, LocalDate value, boolean defaultsSupport, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            SerializerUtils.writeDate(out, value, ZoneId.of("UTC")); // TODO: check
        }
    }

    public static void writeDate(OutputStream out, ZonedDateTime value, boolean defaultsSupport, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            SerializerUtils.writeDate(out, value, ZoneId.of("UTC")); // TODO: check
        }
    }

    // clickhouse type String support
    public static void writeString(OutputStream out, String value, boolean defaultsSupport, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeString(out, convertToString(value));
        }
    }

    public static void writeFixedString(OutputStream out, String value, boolean defaultsSupport, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, int size, String column) throws IOException {
        if (writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeFixedString(out, convertToString(value), size);
        }
    }

    // Int8
    public static void writeInt8(OutputStream out, Byte value, boolean defaultsSupport, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeInt8(out, convertToInteger(value));
        }
    }

    // Int16
    public static void writeInt16(OutputStream out, Short value, boolean defaultsSupport, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeInt16(out, convertToInteger(value));
        }
    }

    // Int32
    public static void writeInt32(OutputStream out, Integer value, boolean defaultsSupport, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeInt32(out, convertToInteger(value));
        }
    }

    // Int64
    public static void writeInt64(OutputStream out, Long value, boolean defaultsSupport, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeInt64(out, convertToInteger(value));
        }
    }

    // Float32
    public static void writeFloat32(OutputStream out, Float value, boolean defaultsSupport, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeFloat32(out, value);
        }
    }

    // Float64
    public static void writeFloat64(OutputStream out, Double value, boolean defaultsSupport, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (writeValuePreamble(out, defaultsSupport, isNullable, dataType, hasDefault, column, value)) {
            BinaryStreamUtils.writeFloat64(out,  value);
        }
    }

}
