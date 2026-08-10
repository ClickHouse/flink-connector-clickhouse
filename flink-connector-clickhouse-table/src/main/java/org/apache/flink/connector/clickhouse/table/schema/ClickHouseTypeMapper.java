package org.apache.flink.connector.clickhouse.table.schema;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;

import org.apache.flink.connector.clickhouse.table.data.ValueConverter;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import java.math.BigInteger;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * The (Flink {@code LogicalType}, {@code ClickHouseColumn}) compatibility matrix of the
 * Table API sink: returns the {@link ValueConverter} that unwraps a Flink-internal value
 * into the plain Java value {@code DataWriter} expects, or throws a
 * {@link TypeMappingException} saying why the pair is rejected.
 *
 * <p>Narrowing is rejected, lossless widening is implicit, and a signed Flink integer never
 * targets an unsigned ClickHouse integer — unsigned columns are reached via the canonical
 * pairs {@code SMALLINT}→{@code UInt8}, {@code INT}→{@code UInt16}, {@code BIGINT}→{@code UInt32},
 * {@code DECIMAL(20,0)}→{@code UInt64}.
 *
 * <p>Every {@link LogicalTypeRoot} is registered — mapped or explicitly rejected — and a
 * guard test asserts exhaustiveness.
 */
public final class ClickHouseTypeMapper {

    /** One matrix row: maps a pair to a converter or throws {@link TypeMappingException}. */
    @FunctionalInterface
    private interface RootRule {
        ValueConverter apply(LogicalType flinkType, ClickHouseColumn target, ZoneId sinkTimezone, String path);
    }

    /** ClickHouse types the sink can write, after unwrapping transparent wrappers. */
    private static final Set<ClickHouseDataType> WRITABLE_TARGETS = EnumSet.of(
            ClickHouseDataType.Bool,
            ClickHouseDataType.Int8, ClickHouseDataType.Int16, ClickHouseDataType.Int32,
            ClickHouseDataType.Int64, ClickHouseDataType.Int128, ClickHouseDataType.Int256,
            ClickHouseDataType.UInt8, ClickHouseDataType.UInt16, ClickHouseDataType.UInt32,
            ClickHouseDataType.UInt64, ClickHouseDataType.UInt128, ClickHouseDataType.UInt256,
            ClickHouseDataType.Float32, ClickHouseDataType.Float64,
            ClickHouseDataType.Decimal, ClickHouseDataType.Decimal32, ClickHouseDataType.Decimal64,
            ClickHouseDataType.Decimal128, ClickHouseDataType.Decimal256,
            ClickHouseDataType.String, ClickHouseDataType.FixedString,
            ClickHouseDataType.UUID, ClickHouseDataType.JSON,
            ClickHouseDataType.Date, ClickHouseDataType.Date32,
            ClickHouseDataType.DateTime, ClickHouseDataType.DateTime64,
            ClickHouseDataType.Array, ClickHouseDataType.Map, ClickHouseDataType.Tuple);

    /**
     * Map key types the sink supports: keys are checkpointed as strings (the state format
     * requires string map keys) and only these types parse back from a string in the
     * client's serializer.
     */
    private static final Set<ClickHouseDataType> STRING_RESTORABLE_MAP_KEY_TARGETS = EnumSet.of(
            ClickHouseDataType.String, ClickHouseDataType.FixedString,
            ClickHouseDataType.Int8, ClickHouseDataType.Int16, ClickHouseDataType.Int32,
            ClickHouseDataType.Int64, ClickHouseDataType.Int128, ClickHouseDataType.Int256,
            ClickHouseDataType.UInt8, ClickHouseDataType.UInt16, ClickHouseDataType.UInt32,
            ClickHouseDataType.UInt64, ClickHouseDataType.UInt128, ClickHouseDataType.UInt256);

    /** Digits of the largest UInt64 (18446744073709551615) — the DECIMAL(20,0) canonical pair. */
    private static final int UINT64_MAX_DIGITS = 20;

    private static final Map<LogicalTypeRoot, RootRule> RULES = buildRules();

    private ClickHouseTypeMapper() {}

    // ------------------------------------------------------------------------------------
    // Entry points
    // ------------------------------------------------------------------------------------

    /**
     * Returns the converter for one column pair, or throws {@link TypeMappingException}.
     *
     * @param flinkType    the Flink column/field type
     * @param column       the introspected ClickHouse column (wrappers still attached)
     * @param sinkTimezone zone in which {@code TIMESTAMP} wall-clock values are interpreted
     * @param path         column path for runtime error messages (e.g. {@code "props value"})
     */
    public static ValueConverter converterFor(LogicalType flinkType, ClickHouseColumn column,
                                              ZoneId sinkTimezone, String path) {
        ClickHouseColumn target = unwrapTransparentWrappers(column);
        checkTargetWritable(target);
        RootRule rule = RULES.get(flinkType.getTypeRoot());
        if (rule == null) {
            // Only reachable on a Flink generation newer than this build's type-root set.
            throw TypeMappingException.mismatch(
                    "Flink type root " + flinkType.getTypeRoot() + " is unknown to this connector build");
        }
        return rule.apply(flinkType, target, sinkTimezone, path);
    }

    /**
     * {@code SimpleAggregateFunction(f, T)} is wire-encoded as its inner type {@code T} and
     * matched as such; {@code LowCardinality}/{@code Nullable} are flags on the column itself.
     */
    public static ClickHouseColumn unwrapTransparentWrappers(ClickHouseColumn column) {
        ClickHouseColumn c = column;
        while (c.getDataType() == ClickHouseDataType.SimpleAggregateFunction
                && c.hasNestedColumn()) {
            c = c.getNestedColumns().get(0);
        }
        return c;
    }

    /** Roots the matrix covers — the guard test asserts this equals all of {@link LogicalTypeRoot}. */
    public static Set<LogicalTypeRoot> registeredRoots() {
        return Collections.unmodifiableSet(RULES.keySet());
    }

    // ------------------------------------------------------------------------------------
    // ClickHouse-side whitelist
    // ------------------------------------------------------------------------------------

    private static void checkTargetWritable(ClickHouseColumn target) {
        ClickHouseDataType type = target.getDataType();
        if (WRITABLE_TARGETS.contains(type)) {
            return;
        }
        throw TypeMappingException.targetUnsupported(unsupportedTargetReason(type));
    }

    private static String unsupportedTargetReason(ClickHouseDataType type) {
        switch (type) {
            case Enum8:
            case Enum16:
                return "see issue #43";
            case Variant:
                return "see issue #60";
            case Time:
            case Time64:
                return "see issue #91";
            default:
                return "no write path and no unambiguous Flink counterpart";
        }
    }

    // ------------------------------------------------------------------------------------
    // Matrix registration — one entry per LogicalTypeRoot
    // ------------------------------------------------------------------------------------

    private static Map<LogicalTypeRoot, RootRule> buildRules() {
        Map<LogicalTypeRoot, RootRule> rules = new EnumMap<>(LogicalTypeRoot.class);

        rules.put(LogicalTypeRoot.BOOLEAN, ClickHouseTypeMapper::mapBoolean);
        rules.put(LogicalTypeRoot.TINYINT, ClickHouseTypeMapper::mapTinyInt);
        rules.put(LogicalTypeRoot.SMALLINT, ClickHouseTypeMapper::mapSmallInt);
        rules.put(LogicalTypeRoot.INTEGER, ClickHouseTypeMapper::mapInt);
        rules.put(LogicalTypeRoot.BIGINT, ClickHouseTypeMapper::mapBigInt);
        rules.put(LogicalTypeRoot.DECIMAL, ClickHouseTypeMapper::mapDecimal);
        rules.put(LogicalTypeRoot.FLOAT, ClickHouseTypeMapper::mapFloat);
        rules.put(LogicalTypeRoot.DOUBLE, ClickHouseTypeMapper::mapDouble);
        rules.put(LogicalTypeRoot.CHAR, ClickHouseTypeMapper::mapCharString);
        rules.put(LogicalTypeRoot.VARCHAR, ClickHouseTypeMapper::mapCharString);
        rules.put(LogicalTypeRoot.DATE, ClickHouseTypeMapper::mapDate);
        rules.put(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, ClickHouseTypeMapper::mapTimestamp);
        rules.put(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE, ClickHouseTypeMapper::mapTimestampLtz);
        rules.put(LogicalTypeRoot.ARRAY, ClickHouseTypeMapper::mapArray);
        rules.put(LogicalTypeRoot.MAP, ClickHouseTypeMapper::mapMap);
        rules.put(LogicalTypeRoot.MULTISET, ClickHouseTypeMapper::mapMultiset);
        rules.put(LogicalTypeRoot.ROW, ClickHouseTypeMapper::mapRow);

        rules.put(LogicalTypeRoot.BINARY, rejected(
                "the sink does not yet support binary types (DataWriter lacks a byte[] write path)"));
        rules.put(LogicalTypeRoot.VARBINARY, rejected(
                "the sink does not yet support binary types (DataWriter lacks a byte[] write path)"));
        rules.put(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE, rejected(
                "ClickHouse Time/Time64 are still experimental and the sink has no write path yet (see issue #91)"));
        rules.put(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE, rejected(
                "TIMESTAMP WITH TIME ZONE is not supported — use TIMESTAMP or TIMESTAMP_LTZ"));
        rules.put(LogicalTypeRoot.INTERVAL_YEAR_MONTH, rejected(
                "INTERVAL cannot be a ClickHouse table column"));
        rules.put(LogicalTypeRoot.INTERVAL_DAY_TIME, rejected(
                "INTERVAL cannot be a ClickHouse table column"));
        rules.put(LogicalTypeRoot.NULL, rejected(
                "the NULL type is a planner artifact — CAST the value to a real type"));
        rules.put(LogicalTypeRoot.SYMBOL, rejected(
                "the SYMBOL type is a planner artifact — CAST the value to a real type"));
        rules.put(LogicalTypeRoot.RAW, rejected(
                "RAW is opaque Java bytes — CAST the value to a concrete SQL type"));
        rules.put(LogicalTypeRoot.DISTINCT_TYPE, rejected(
                "DISTINCT types are not supported — CAST the value to its source type"));
        rules.put(LogicalTypeRoot.STRUCTURED_TYPE, rejected(
                "structured types are not supported — use ROW instead"));
        rules.put(LogicalTypeRoot.UNRESOLVED, rejected(
                "the type is unresolved — this is a planner inconsistency"));

        return rules;
    }

    private static RootRule rejected(String reason) {
        return (flinkType, target, zone, path) -> {
            throw TypeMappingException.mismatch(reason);
        };
    }

    // ------------------------------------------------------------------------------------
    // Scalar rows
    // ------------------------------------------------------------------------------------

    private static ValueConverter mapBoolean(LogicalType flinkType, ClickHouseColumn target,
                                             ZoneId zone, String path) {
        if (target.getDataType() == ClickHouseDataType.Bool) {
            return value -> value;
        }
        throw noConversion(flinkType, "Bool");
    }

    private static ValueConverter mapTinyInt(LogicalType flinkType, ClickHouseColumn target,
                                             ZoneId zone, String path) {
        switch (target.getDataType()) {
            case Int8:   return value -> value;
            case Int16:  return value -> ((Byte) value).shortValue();
            case Int32:  return value -> ((Byte) value).intValue();
            case Int64:  return value -> ((Byte) value).longValue();
            case Int128:
            case Int256: return value -> BigInteger.valueOf((Byte) value);
            default:     throw noConversion(flinkType, "Int8 (or a wider signed integer)");
        }
    }

    private static ValueConverter mapSmallInt(LogicalType flinkType, ClickHouseColumn target,
                                              ZoneId zone, String path) {
        switch (target.getDataType()) {
            case Int16:  return value -> value;
            case UInt8:  return value -> ((Short) value).intValue();
            case Int32:  return value -> ((Short) value).intValue();
            case Int64:  return value -> ((Short) value).longValue();
            case Int128:
            case Int256: return value -> BigInteger.valueOf((Short) value);
            default:     throw noConversion(flinkType, "Int16, UInt8 (or a wider signed integer)");
        }
    }

    private static ValueConverter mapInt(LogicalType flinkType, ClickHouseColumn target,
                                         ZoneId zone, String path) {
        switch (target.getDataType()) {
            case Int32:
            case UInt16: return value -> value;
            case Int64:  return value -> ((Integer) value).longValue();
            case Int128:
            case Int256: return value -> BigInteger.valueOf((Integer) value);
            default:     throw noConversion(flinkType, "Int32, UInt16 (or a wider signed integer)");
        }
    }

    private static ValueConverter mapBigInt(LogicalType flinkType, ClickHouseColumn target,
                                            ZoneId zone, String path) {
        switch (target.getDataType()) {
            case Int64:
            case UInt32: return value -> value;
            case Int128:
            case Int256: return value -> BigInteger.valueOf((Long) value);
            default:     throw noConversion(flinkType, "Int64, UInt32, Int128, Int256");
        }
    }

    private static ValueConverter mapDecimal(LogicalType flinkType, ClickHouseColumn target,
                                             ZoneId zone, String path) {
        DecimalType decimalType = (DecimalType) flinkType;
        int precision = decimalType.getPrecision();
        int scale = decimalType.getScale();
        switch (target.getDataType()) {
            case Decimal:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal256:
                checkDecimalFits(precision, scale, target);
                return value -> ((DecimalData) value).toBigDecimal();
            case Int128:
            case Int256:
            case UInt64:
            case UInt128:
            case UInt256:
                checkDecimalFitsInteger(precision, scale, target);
                return value -> ((DecimalData) value).toBigDecimal().toBigIntegerExact();
            default:
                throw noConversion(flinkType, "Decimal(p,s), Int128, Int256, UInt64, UInt128, UInt256");
        }
    }

    private static void checkDecimalFits(int precision, int scale, ClickHouseColumn target) {
        int targetPrecision = target.getPrecision();
        int targetScale = target.getScale();
        if (targetScale < scale) {
            throw TypeMappingException.mismatch(String.format(
                    "scale %d exceeds the column's scale %d", scale, targetScale));
        }
        if (targetPrecision - targetScale < precision - scale) {
            throw TypeMappingException.mismatch(String.format(
                    "%d integer digits exceed the column's %d integer digits",
                    precision - scale, targetPrecision - targetScale));
        }
    }

    private static void checkDecimalFitsInteger(int precision, int scale, ClickHouseColumn target) {
        if (scale != 0) {
            throw TypeMappingException.mismatch(String.format(
                    "scale %d has a fractional part; only DECIMAL(p, 0) can be written to an integer column",
                    scale));
        }
        if (target.getDataType() == ClickHouseDataType.UInt64 && precision > UINT64_MAX_DIGITS) {
            throw TypeMappingException.mismatch(String.format(
                    "precision %d exceeds UInt64's %d digits", precision, UINT64_MAX_DIGITS));
        }
    }

    private static ValueConverter mapFloat(LogicalType flinkType, ClickHouseColumn target,
                                           ZoneId zone, String path) {
        switch (target.getDataType()) {
            case Float32: return value -> value;
            case Float64: return value -> ((Float) value).doubleValue();
            default:      throw noConversion(flinkType, "Float32, Float64");
        }
    }

    private static ValueConverter mapDouble(LogicalType flinkType, ClickHouseColumn target,
                                            ZoneId zone, String path) {
        if (target.getDataType() == ClickHouseDataType.Float64) {
            return value -> value;
        }
        throw noConversion(flinkType, "Float64");
    }

    private static ValueConverter mapCharString(LogicalType flinkType, ClickHouseColumn target,
                                                ZoneId zone, String path) {
        switch (target.getDataType()) {
            case String:
            case JSON:
                return Object::toString;
            case FixedString:
                // Byte length is checked at write time by DataWriter.writeFixedString.
                return Object::toString;
            case UUID:
                return uuidConverter(path);
            default:
                throw noConversion(flinkType, "String, FixedString(n), UUID, JSON");
        }
    }

    private static ValueConverter uuidConverter(String path) {
        return value -> {
            String text = value.toString();
            try {
                return UUID.fromString(text);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "Column '" + path + "': value is not a valid UUID: " + text, e);
            }
        };
    }

    private static ValueConverter mapDate(LogicalType flinkType, ClickHouseColumn target,
                                          ZoneId zone, String path) {
        switch (target.getDataType()) {
            case Date32:
                return value -> LocalDate.ofEpochDay((Integer) value);
            case Date:
                return dateRangeCheckedConverter(path);
            default:
                throw noConversion(flinkType, "Date, Date32");
        }
    }

    /** ClickHouse {@code Date} is UInt16 epoch days: 1970-01-01 .. 2149-06-06. */
    private static ValueConverter dateRangeCheckedConverter(String path) {
        return value -> {
            int epochDay = (Integer) value;
            if (epochDay < 0 || epochDay > 65535) {
                throw new IllegalArgumentException(
                        "Column '" + path + "': DATE value " + LocalDate.ofEpochDay(epochDay)
                        + " is outside the ClickHouse Date range 1970-01-01..2149-06-06"
                        + " — use Date32 for a wider range");
            }
            return LocalDate.ofEpochDay(epochDay);
        };
    }

    private static ValueConverter mapTimestamp(LogicalType flinkType, ClickHouseColumn target,
                                               ZoneId zone, String path) {
        int precision = ((TimestampType) flinkType).getPrecision();
        checkDateTimeTargetFits(flinkType, target, precision);
        // Wall-clock value: interpret in sink.timezone, write instant-exactly — this keeps
        // non-UTC DateTime(tz) columns correct.
        return value -> ZonedDateTime.of(((TimestampData) value).toLocalDateTime(), zone);
    }

    private static ValueConverter mapTimestampLtz(LogicalType flinkType, ClickHouseColumn target,
                                                  ZoneId zone, String path) {
        int precision = ((LocalZonedTimestampType) flinkType).getPrecision();
        checkDateTimeTargetFits(flinkType, target, precision);
        // Instant value: zone choice is irrelevant for the wire bytes; UTC keeps state small.
        return value -> ZonedDateTime.ofInstant(((TimestampData) value).toInstant(), ZoneOffset.UTC);
    }

    /** The target must be DateTime (scale 0) or DateTime64(s) with s >= the Flink precision. */
    private static void checkDateTimeTargetFits(LogicalType flinkType, ClickHouseColumn target,
                                                int precision) {
        switch (target.getDataType()) {
            case DateTime:
            case DateTime64:
                int columnScale = target.getDataType() == ClickHouseDataType.DateTime
                        ? 0 : target.getScale();
                if (precision > columnScale) {
                    throw TypeMappingException.mismatch(String.format(
                            "precision %d exceeds the column's scale %d", precision, columnScale));
                }
                return;
            default:
                throw noConversion(flinkType, "DateTime, DateTime64(s) with s >= the Flink precision");
        }
    }

    // ------------------------------------------------------------------------------------
    // Composite rows — recursive; nullability is validated structurally
    // ------------------------------------------------------------------------------------

    private static ValueConverter mapArray(LogicalType flinkType, ClickHouseColumn target,
                                           ZoneId zone, String path) {
        if (target.getDataType() != ClickHouseDataType.Array) {
            throw noConversion(flinkType, "Array(T)");
        }
        LogicalType elementType = ((ArrayType) flinkType).getElementType();
        ClickHouseColumn elementColumn = unwrapTransparentWrappers(target.getNestedColumns().get(0));
        if (elementType.isNullable() && !elementColumn.isNullable()) {
            throw TypeMappingException.mismatch(String.format(
                    "the Flink array element type %s is nullable but the ClickHouse element type %s "
                    + "is not Nullable — declare the element NOT NULL or make the element Nullable",
                    elementType.asSummaryString(), elementColumn.getOriginalTypeName()));
        }
        ValueConverter elementConverter = converterWithContext(
                elementType, elementColumn, zone, path + " element", "array element");
        ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        return value -> convertArray((ArrayData) value, elementGetter, elementConverter);
    }

    private static List<Object> convertArray(ArrayData array, ArrayData.ElementGetter getter,
                                             ValueConverter elementConverter) {
        List<Object> result = new ArrayList<>(array.size());
        for (int i = 0; i < array.size(); i++) {
            Object element = getter.getElementOrNull(array, i);
            result.add(element == null ? null : elementConverter.convert(element));
        }
        return result;
    }

    private static ValueConverter mapMap(LogicalType flinkType, ClickHouseColumn target,
                                         ZoneId zone, String path) {
        if (target.getDataType() != ClickHouseDataType.Map) {
            throw noConversion(flinkType, "Map(K, V)");
        }
        MapType mapType = (MapType) flinkType;
        ValueConverter valueConverter = buildMapValueConverter(
                mapType.getValueType(), target.getValueInfo(), zone, path);
        return buildMapConverter(mapType.getKeyType(), mapType.getValueType(), valueConverter,
                target, zone, path);
    }

    /** MULTISET&lt;T&gt; is a map from T to a non-null int count, matched against {@code Map(T', UInt64)}. */
    private static ValueConverter mapMultiset(LogicalType flinkType, ClickHouseColumn target,
                                              ZoneId zone, String path) {
        if (target.getDataType() != ClickHouseDataType.Map) {
            throw noConversion(flinkType, "Map(T, UInt64)");
        }
        checkMultisetCountTarget(target);
        LogicalType elementType = ((MultisetType) flinkType).getElementType();
        // DataWriter's UInt64 write path takes a Long; the count getter yields an Integer.
        ValueConverter countConverter = count -> ((Integer) count).longValue();
        return buildMapConverter(elementType, new IntType(false), countConverter, target, zone, path);
    }

    private static void checkMultisetCountTarget(ClickHouseColumn target) {
        ClickHouseColumn valueColumn = unwrapTransparentWrappers(target.getValueInfo());
        if (valueColumn.getDataType() != ClickHouseDataType.UInt64 || valueColumn.isNullable()) {
            throw TypeMappingException.mismatch(String.format(
                    "MULTISET counts require a Map value type of exactly UInt64, found %s",
                    target.getValueInfo().getOriginalTypeName()));
        }
    }

    private static ValueConverter buildMapConverter(LogicalType keyType, LogicalType valueType,
                                                    ValueConverter valueConverter,
                                                    ClickHouseColumn target, ZoneId zone, String path) {
        ValueConverter keyConverter = buildMapKeyConverter(keyType, target.getKeyInfo(), zone, path);
        ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(keyType);
        ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
        return value -> convertMap((MapData) value, keyGetter, valueGetter,
                keyConverter, valueConverter, path);
    }

    /**
     * Keys are checkpointed as the payload map's string keys, so the converter stringifies
     * the key and the serializer parses it back (whitelisted types only). Flink SQL marks
     * every MAP key type nullable, so key nullability is enforced on the data at runtime,
     * not on the type.
     */
    private static ValueConverter buildMapKeyConverter(LogicalType keyType, ClickHouseColumn keyColumn,
                                                       ZoneId zone, String path) {
        ClickHouseColumn effectiveKey = unwrapTransparentWrappers(keyColumn);
        if (effectiveKey.isNullable()) {
            throw TypeMappingException.mismatch("ClickHouse Map keys cannot be Nullable");
        }
        if (!STRING_RESTORABLE_MAP_KEY_TARGETS.contains(effectiveKey.getDataType())) {
            throw TypeMappingException.mismatch(String.format(
                    "ClickHouse Map key type %s is not supported by the sink — map keys are "
                    + "checkpointed as strings and %s cannot be restored from a string",
                    effectiveKey.getOriginalTypeName(), effectiveKey.getDataType()));
        }
        ValueConverter rawKeyConverter = converterWithContext(
                keyType, effectiveKey, zone, path + " key", "map key");
        return value -> String.valueOf(rawKeyConverter.convert(value));
    }

    /**
     * Nullable Map values are rejected: the client-side serializer never writes their
     * non-null marker, so a byte-exact stream is impossible (same gap for Tuple elements).
     */
    private static ValueConverter buildMapValueConverter(LogicalType valueType, ClickHouseColumn valueColumn,
                                                         ZoneId zone, String path) {
        ClickHouseColumn effectiveValue = unwrapTransparentWrappers(valueColumn);
        if (effectiveValue.isNullable()) {
            throw TypeMappingException.mismatch(String.format(
                    "Nullable Map values (%s) are not supported by the sink's serializer — "
                    + "use a non-Nullable value type",
                    valueColumn.getOriginalTypeName()));
        }
        if (valueType.isNullable()) {
            throw TypeMappingException.mismatch(String.format(
                    "the Flink map value type %s is nullable but the ClickHouse value type %s "
                    + "is not Nullable — declare the value NOT NULL",
                    valueType.asSummaryString(), valueColumn.getOriginalTypeName()));
        }
        return converterWithContext(valueType, effectiveValue, zone, path + " value", "map value");
    }

    private static Map<String, Object> convertMap(MapData map,
                                                  ArrayData.ElementGetter keyGetter,
                                                  ArrayData.ElementGetter valueGetter,
                                                  ValueConverter keyConverter,
                                                  ValueConverter valueConverter,
                                                  String path) {
        ArrayData keys = map.keyArray();
        ArrayData values = map.valueArray();
        Map<String, Object> result = new LinkedHashMap<>(map.size());
        for (int i = 0; i < map.size(); i++) {
            Object key = keyGetter.getElementOrNull(keys, i);
            if (key == null) {
                throw new IllegalArgumentException(
                        "Column '" + path + "': null map key cannot be written to ClickHouse");
            }
            Object value = valueGetter.getElementOrNull(values, i);
            if (value == null) {
                throw new IllegalArgumentException(
                        "Column '" + path + "': null map value cannot be written to a non-Nullable "
                        + "ClickHouse Map value type");
            }
            result.put((String) keyConverter.convert(key), valueConverter.convert(value));
        }
        return result;
    }

    /**
     * ROW fields match Tuple elements positionally. Nullable Tuple elements are rejected
     * for the same serializer gap as Nullable Map values ({@link #buildMapValueConverter}).
     */
    private static ValueConverter mapRow(LogicalType flinkType, ClickHouseColumn target,
                                         ZoneId zone, String path) {
        if (target.getDataType() != ClickHouseDataType.Tuple) {
            throw noConversion(flinkType, "Tuple(...)");
        }
        RowType rowType = (RowType) flinkType;
        List<ClickHouseColumn> elements = target.getNestedColumns();
        if (rowType.getFieldCount() != elements.size()) {
            throw TypeMappingException.mismatch(String.format(
                    "ROW has %d fields but the Tuple has %d elements",
                    rowType.getFieldCount(), elements.size()));
        }
        RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[rowType.getFieldCount()];
        ValueConverter[] fieldConverters = new ValueConverter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            RowType.RowField field = rowType.getFields().get(i);
            fieldGetters[i] = RowData.createFieldGetter(field.getType(), i);
            fieldConverters[i] = buildRowFieldConverter(field, elements.get(i), i, zone, path);
        }
        return value -> convertRow((RowData) value, fieldGetters, fieldConverters, path);
    }

    private static ValueConverter buildRowFieldConverter(RowType.RowField field, ClickHouseColumn element,
                                                         int position, ZoneId zone, String path) {
        ClickHouseColumn effective = unwrapTransparentWrappers(element);
        if (effective.isNullable()) {
            throw TypeMappingException.mismatch(String.format(
                    "Nullable Tuple elements (%s at position %d) are not supported by the "
                    + "sink's serializer — use a non-Nullable element type",
                    element.getOriginalTypeName(), position + 1));
        }
        if (field.getType().isNullable()) {
            throw TypeMappingException.mismatch(String.format(
                    "the Flink ROW field '%s' is nullable but the ClickHouse Tuple element %s "
                    + "is not Nullable — declare the field NOT NULL",
                    field.getName(), element.getOriginalTypeName()));
        }
        return converterWithContext(field.getType(), effective, zone,
                path + "." + field.getName(), "ROW field '" + field.getName() + "'");
    }

    private static Object[] convertRow(RowData row, RowData.FieldGetter[] getters,
                                       ValueConverter[] converters, String path) {
        Object[] result = new Object[getters.length];
        for (int i = 0; i < getters.length; i++) {
            Object field = getters[i].getFieldOrNull(row);
            if (field == null) {
                throw new IllegalArgumentException(
                        "Column '" + path + "': null ROW field " + (i + 1)
                        + " cannot be written to a non-Nullable Tuple element");
            }
            result[i] = converters[i].convert(field);
        }
        return result;
    }

    // ------------------------------------------------------------------------------------
    // Shared helpers
    // ------------------------------------------------------------------------------------

    /** Recurses into a nested pair, prefixing mismatch reasons with the structural context. */
    private static ValueConverter converterWithContext(LogicalType flinkType, ClickHouseColumn column,
                                                       ZoneId zone, String path, String context) {
        try {
            return converterFor(flinkType, column, zone, path);
        } catch (TypeMappingException e) {
            if (e.getKind() == TypeMappingException.Kind.TARGET_UNSUPPORTED) {
                throw e;
            }
            throw TypeMappingException.mismatch(context + ": " + e.getMessage());
        }
    }

    private static TypeMappingException noConversion(LogicalType flinkType, String supportedTargets) {
        return TypeMappingException.mismatch(String.format(
                "no supported conversion; supported ClickHouse types for %s: %s",
                flinkType.asSummaryString(), supportedTargets));
    }
}
