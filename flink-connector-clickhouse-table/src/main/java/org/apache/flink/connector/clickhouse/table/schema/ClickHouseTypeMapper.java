package org.apache.flink.connector.clickhouse.table.schema;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.format.BinaryStreamUtils;

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
import java.nio.charset.StandardCharsets;
import java.time.Instant;
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

import static com.clickhouse.utils.writer.DataWriter.unwrapTransparentWrappers;

/**
 * The (Flink {@code LogicalType}, {@code ClickHouseColumn}) compatibility matrix of the
 * Table API sink: returns the {@link ValueConverter} that turns a Flink-internal value into
 * the plain Java value {@code DataWriter} expects, or throws a {@link TypeMappingException}
 * saying why the pair is rejected.
 *
 * <p>Narrowing is rejected, lossless widening is implicit, and a signed Flink integer never
 * targets an unsigned ClickHouse integer — unsigned columns are reached via the canonical
 * pairs {@code SMALLINT}→{@code UInt8}, {@code INT}→{@code UInt16}, {@code BIGINT}→{@code UInt32},
 * {@code DECIMAL(20,0)}→{@code UInt64}, whose values are range-checked per record: the wire
 * format cannot represent a sign, and the client's writer would otherwise fail without the
 * column name or (UInt64 inside composites) wrap the value silently.
 *
 * <p>{@code build*Converter} methods run once per column at planning time; {@code toPayload*}
 * methods run per record on the TaskManager. Wrapper shedding is shared with the write path
 * (DataWriter#unwrapTransparentWrappers).
 */
public final class ClickHouseTypeMapper {

    /** One matrix row: maps a pair to a converter or throws {@link TypeMappingException}. */
    @FunctionalInterface
    private interface RootRule {
        ValueConverter apply(LogicalType flinkType, ClickHouseColumn target, ZoneId sinkTimezone, String path);
    }

    /** ClickHouse types the sink can write, after unwrapping transparent wrappers. */
    // Package-private so the DataWriter cross-check test can pin it against the real dispatch.
    static final Set<ClickHouseDataType> WRITABLE_TARGETS = EnumSet.of(
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
     * client's serializer. UInt64 is absent because client-v2's SerializerUtils hardcodes
     * Long.parseLong for it, so keys above 2^63-1 fail.
     */
    private static final Set<ClickHouseDataType> STRING_RESTORABLE_MAP_KEY_TARGETS = EnumSet.of(
            ClickHouseDataType.String, ClickHouseDataType.FixedString,
            ClickHouseDataType.Int8, ClickHouseDataType.Int16, ClickHouseDataType.Int32,
            ClickHouseDataType.Int64, ClickHouseDataType.Int128, ClickHouseDataType.Int256,
            ClickHouseDataType.UInt8, ClickHouseDataType.UInt16, ClickHouseDataType.UInt32,
            ClickHouseDataType.UInt128, ClickHouseDataType.UInt256);

    // Range bounds derive from the client that owns the wire format, so a client bump moves them too.

    /** Digits of the largest UInt64 (18446744073709551615) — the DECIMAL(20,0) canonical pair. */
    private static final int UINT64_MAX_DIGITS = ClickHouseDataType.UInt64.getMaxPrecision();

    /** The largest UInt64 itself — 20 digits admit values above it, so writes re-check. */
    private static final BigInteger UINT64_MAX = BinaryStreamUtils.U_INT64_MAX;

    /** ClickHouse {@code Date} is UInt16 epoch days, so 2149-06-06 is its last day. */
    private static final int DATE_MAX_EPOCH_DAY = BinaryStreamUtils.U_INT16_MAX;

    /** ClickHouse {@code Date32} covers 1900-01-01..2299-12-31, as signed epoch days. */
    private static final int DATE32_MIN_EPOCH_DAY = BinaryStreamUtils.DATE32_MIN;
    private static final int DATE32_MAX_EPOCH_DAY = BinaryStreamUtils.DATE32_MAX;

    /** ClickHouse {@code DateTime} is UInt32 epoch seconds, ending 2106-02-07T06:28:15Z. */
    private static final long DATETIME_MAX_EPOCH_SECOND = BinaryStreamUtils.U_INT32_MAX;

    /** ClickHouse {@code DateTime64} covers 1900-01-01T00:00:00Z..2299-12-31T23:59:59Z. */
    private static final long DATETIME64_MIN_EPOCH_SECOND = BinaryStreamUtils.DATETIME64_MIN;
    private static final long DATETIME64_MAX_EPOCH_SECOND = BinaryStreamUtils.DATETIME64_MAX;

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

    /**
     * The client serializer can wire-encode SimpleAggregateFunction only as a top-level column;
     * inside a composite it has no case for it and every record would fail on the TaskManager.
     */
    private static ClickHouseColumn rejectNestedSimpleAggregateFunction(ClickHouseColumn column,
                                                                        String position) {
        if (column.getDataType() == ClickHouseDataType.SimpleAggregateFunction) {
            throw TypeMappingException.targetUnsupported(String.format(
                    "SimpleAggregateFunction is only writable as a top-level column; found as %s",
                    position));
        }
        return column;
    }

    // ------------------------------------------------------------------------------------
    // Matrix registration — one entry per LogicalTypeRoot
    // ------------------------------------------------------------------------------------

    private static Map<LogicalTypeRoot, RootRule> buildRules() {
        Map<LogicalTypeRoot, RootRule> rules = new EnumMap<>(LogicalTypeRoot.class);

        rules.put(LogicalTypeRoot.BOOLEAN, ClickHouseTypeMapper::buildBooleanConverter);
        rules.put(LogicalTypeRoot.TINYINT, signedIntegerRule(
                ClickHouseDataType.Int8, null, 0L,
                EnumSet.of(ClickHouseDataType.Int16, ClickHouseDataType.Int32, ClickHouseDataType.Int64,
                        ClickHouseDataType.Int128, ClickHouseDataType.Int256),
                "Int8 (or a wider signed integer)"));
        rules.put(LogicalTypeRoot.SMALLINT, signedIntegerRule(
                ClickHouseDataType.Int16, ClickHouseDataType.UInt8, BinaryStreamUtils.U_INT8_MAX,
                EnumSet.of(ClickHouseDataType.Int32, ClickHouseDataType.Int64,
                        ClickHouseDataType.Int128, ClickHouseDataType.Int256),
                "Int16, UInt8 (or a wider signed integer)"));
        rules.put(LogicalTypeRoot.INTEGER, signedIntegerRule(
                ClickHouseDataType.Int32, ClickHouseDataType.UInt16, BinaryStreamUtils.U_INT16_MAX,
                EnumSet.of(ClickHouseDataType.Int64, ClickHouseDataType.Int128, ClickHouseDataType.Int256),
                "Int32, UInt16 (or a wider signed integer)"));
        rules.put(LogicalTypeRoot.BIGINT, signedIntegerRule(
                ClickHouseDataType.Int64, ClickHouseDataType.UInt32, BinaryStreamUtils.U_INT32_MAX,
                EnumSet.of(ClickHouseDataType.Int128, ClickHouseDataType.Int256),
                "Int64, UInt32, Int128, Int256"));
        rules.put(LogicalTypeRoot.DECIMAL, ClickHouseTypeMapper::buildDecimalConverter);
        rules.put(LogicalTypeRoot.FLOAT, ClickHouseTypeMapper::buildFloatConverter);
        rules.put(LogicalTypeRoot.DOUBLE, ClickHouseTypeMapper::buildDoubleConverter);
        rules.put(LogicalTypeRoot.CHAR, ClickHouseTypeMapper::buildStringConverter);
        rules.put(LogicalTypeRoot.VARCHAR, ClickHouseTypeMapper::buildStringConverter);
        rules.put(LogicalTypeRoot.DATE, ClickHouseTypeMapper::buildDateConverter);
        rules.put(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, ClickHouseTypeMapper::buildTimestampConverter);
        rules.put(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE, ClickHouseTypeMapper::buildTimestampLtzConverter);
        rules.put(LogicalTypeRoot.ARRAY, ClickHouseTypeMapper::buildArrayConverter);
        rules.put(LogicalTypeRoot.MAP, ClickHouseTypeMapper::buildMapConverter);
        rules.put(LogicalTypeRoot.MULTISET, ClickHouseTypeMapper::buildMultisetConverter);
        rules.put(LogicalTypeRoot.ROW, ClickHouseTypeMapper::buildRowConverter);

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

    private static ValueConverter buildBooleanConverter(LogicalType flinkType, ClickHouseColumn target,
                                                       ZoneId zone, String path) {
        if (target.getDataType() == ClickHouseDataType.Bool) {
            return value -> value;
        }
        throw noConversion(flinkType, "Bool");
    }

    /** The shared rule shape of the four signed Flink integers; sources are all {@code Number}s. */
    private static RootRule signedIntegerRule(ClickHouseDataType identityTarget,
                                              ClickHouseDataType unsignedTarget, long unsignedMax,
                                              Set<ClickHouseDataType> wideningTargets,
                                              String supportedTargets) {
        return (flinkType, target, zone, path) -> {
            ClickHouseDataType targetType = target.getDataType();
            if (targetType == identityTarget) {
                return value -> value;
            }
            if (targetType == unsignedTarget) {
                // DataWriter takes UInt8/UInt16 as int and UInt32 as long.
                return unsignedMax <= BinaryStreamUtils.U_INT16_MAX
                        ? value -> (int) checkUnsignedRange(((Number) value).longValue(),
                                unsignedMax, targetType.name(), path)
                        : value -> checkUnsignedRange(((Number) value).longValue(),
                                unsignedMax, targetType.name(), path);
            }
            if (wideningTargets.contains(targetType)) {
                switch (targetType) {
                    case Int16:  return value -> ((Number) value).shortValue();
                    case Int32:  return value -> ((Number) value).intValue();
                    case Int64:  return value -> ((Number) value).longValue();
                    default:     return value -> BigInteger.valueOf(((Number) value).longValue());
                }
            }
            throw noConversion(flinkType, supportedTargets);
        };
    }

    private static ValueConverter buildDecimalConverter(LogicalType flinkType, ClickHouseColumn target,
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
                checkDecimalFitsInteger(precision, scale, target);
                return value -> ((DecimalData) value).toBigDecimal().toBigIntegerExact();
            case UInt64:
            case UInt128:
            case UInt256:
                checkDecimalFitsInteger(precision, scale, target);
                return buildRangeCheckedUnsignedDecimalConverter(target.getDataType(), path);
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

    /**
     * DECIMAL(p, 0) → UInt64/UInt128/UInt256: a sign never fits an unsigned column, and
     * UInt64's maximum sits below the 20-digit precision the planning check admits.
     */
    private static ValueConverter buildRangeCheckedUnsignedDecimalConverter(ClickHouseDataType targetType,
                                                                            String path) {
        return value -> {
            BigInteger integer = ((DecimalData) value).toBigDecimal().toBigIntegerExact();
            if (integer.signum() < 0) {
                throw new IllegalArgumentException(
                        "Column '" + path + "': value " + integer
                        + " is negative and cannot be written to the unsigned type " + targetType);
            }
            if (targetType == ClickHouseDataType.UInt64 && integer.compareTo(UINT64_MAX) > 0) {
                throw new IllegalArgumentException(
                        "Column '" + path + "': value " + integer
                        + " is outside the UInt64 range 0.." + UINT64_MAX);
            }
            return integer;
        };
    }

    private static ValueConverter buildFloatConverter(LogicalType flinkType, ClickHouseColumn target,
                                                      ZoneId zone, String path) {
        switch (target.getDataType()) {
            case Float32: return value -> value;
            case Float64: return value -> ((Float) value).doubleValue();
            default:      throw noConversion(flinkType, "Float32, Float64");
        }
    }

    private static ValueConverter buildDoubleConverter(LogicalType flinkType, ClickHouseColumn target,
                                                       ZoneId zone, String path) {
        if (target.getDataType() == ClickHouseDataType.Float64) {
            return value -> value;
        }
        throw noConversion(flinkType, "Float64");
    }

    /** {@code CHAR}/{@code VARCHAR} arrive as {@link StringData}; every target starts from its text. */
    private static ValueConverter buildStringConverter(LogicalType flinkType, ClickHouseColumn target,
                                                       ZoneId zone, String path) {
        switch (target.getDataType()) {
            case String:
            case JSON:
                return Object::toString;
            case FixedString:
                return buildFixedStringConverter(target.getPrecision(), path);
            case UUID:
                return buildUuidConverter(path);
            default:
                throw noConversion(flinkType, "String, FixedString(n), UUID, JSON");
        }
    }

    /** The write-time length check throws without the column name, so enforce n here instead. */
    private static ValueConverter buildFixedStringConverter(int maxBytes, String path) {
        return value -> {
            String text = value.toString();
            int byteLength = utf8ByteLength(text);
            if (byteLength > maxBytes) {
                throw new IllegalArgumentException(String.format(
                        "Column '%s': value of %d bytes does not fit FixedString(%d): %s",
                        path, byteLength, maxBytes, text));
            }
            return text;
        };
    }

    /** Allocation-free for ASCII (the per-record hot path); the writer re-encodes anyway. */
    private static int utf8ByteLength(String text) {
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) >= 0x80) {
                return text.getBytes(StandardCharsets.UTF_8).length;
            }
        }
        return text.length();
    }

    private static ValueConverter buildUuidConverter(String path) {
        return value -> {
            String text = value.toString();
            // fromString also zero-expands forms like '1-1-1-1-1'; accept only the canonical form.
            if (!isCanonicalUuid(text)) {
                throw new IllegalArgumentException(
                        "Column '" + path + "': value is not a valid UUID: " + text);
            }
            return UUID.fromString(text);
        };
    }

    private static boolean isCanonicalUuid(String text) {
        if (text.length() != 36) {
            return false;
        }
        for (int i = 0; i < 36; i++) {
            char c = text.charAt(i);
            if (i == 8 || i == 13 || i == 18 || i == 23) {
                if (c != '-') {
                    return false;
                }
            } else if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
                return false;
            }
        }
        return true;
    }

    private static ValueConverter buildDateConverter(LogicalType flinkType, ClickHouseColumn target,
                                                     ZoneId zone, String path) {
        switch (target.getDataType()) {
            case Date32:
                return rangeCheckedEpochDayConverter(path, DATE32_MIN_EPOCH_DAY, DATE32_MAX_EPOCH_DAY,
                        "Date32 range 1900-01-01..2299-12-31");
            case Date:
                return rangeCheckedEpochDayConverter(path, 0, DATE_MAX_EPOCH_DAY,
                        "Date range 1970-01-01..2149-06-06 — use Date32 for a wider range");
            default:
                throw noConversion(flinkType, "Date, Date32");
        }
    }

    /** The client writes days as raw UInt16/Int32, so an out-of-range day would be stored wrapped. */
    private static ValueConverter rangeCheckedEpochDayConverter(String path, int minEpochDay,
                                                                int maxEpochDay, String rangeText) {
        return value -> {
            int epochDay = (Integer) value;
            if (epochDay < minEpochDay || epochDay > maxEpochDay) {
                throw new IllegalArgumentException(
                        "Column '" + path + "': DATE value " + LocalDate.ofEpochDay(epochDay)
                        + " is outside the ClickHouse " + rangeText);
            }
            return LocalDate.ofEpochDay(epochDay);
        };
    }

    /** A wall-clock value: interpreted in sink.timezone, written instant-exactly. */
    private static ValueConverter buildTimestampConverter(LogicalType flinkType, ClickHouseColumn target,
                                                          ZoneId zone, String path) {
        checkDateTimeTargetFits(flinkType, target, ((TimestampType) flinkType).getPrecision());
        return rangeCheckedDateTimeConverter(target, path,
                value -> ZonedDateTime.of(((TimestampData) value).toLocalDateTime(), zone));
    }

    /** An instant: the zone cannot change the wire bytes, and UTC keeps the state small. */
    private static ValueConverter buildTimestampLtzConverter(LogicalType flinkType, ClickHouseColumn target,
                                                             ZoneId zone, String path) {
        checkDateTimeTargetFits(flinkType, target, ((LocalZonedTimestampType) flinkType).getPrecision());
        return rangeCheckedDateTimeConverter(target, path,
                value -> ZonedDateTime.ofInstant(((TimestampData) value).toInstant(), ZoneOffset.UTC));
    }

    /**
     * Wraps a timestamp converter with the target's instant range: DateTime is UInt32 epoch
     * seconds (the client's writer rejects the rest without naming the column) and DateTime64
     * spans 1900..2299 — less at scale 9, where the client's Int64 tick math wraps silently.
     */
    private static ValueConverter rangeCheckedDateTimeConverter(ClickHouseColumn target, String path,
                                                                ValueConverter toZonedDateTime) {
        boolean isDateTime64 = target.getDataType() == ClickHouseDataType.DateTime64;
        long minEpochSecond = isDateTime64 ? DATETIME64_MIN_EPOCH_SECOND : 0L;
        long maxEpochSecond = isDateTime64
                ? Math.min(DATETIME64_MAX_EPOCH_SECOND, maxTickSafeEpochSecond(target.getScale()))
                : DATETIME_MAX_EPOCH_SECOND;
        String targetName = isDateTime64 ? "DateTime64" : "DateTime";
        String range = Instant.ofEpochSecond(minEpochSecond) + ".." + Instant.ofEpochSecond(maxEpochSecond);
        return value -> {
            ZonedDateTime converted = (ZonedDateTime) toZonedDateTime.convert(value);
            long epochSecond = converted.toEpochSecond();
            if (epochSecond < minEpochSecond || epochSecond > maxEpochSecond) {
                throw new IllegalArgumentException(
                        "Column '" + path + "': TIMESTAMP value " + converted
                        + " is outside the ClickHouse " + targetName + " range " + range);
            }
            return converted;
        };
    }

    /** The largest epoch second whose DateTime64 ticks (second × 10^scale + fraction) fit an Int64. */
    private static long maxTickSafeEpochSecond(int scale) {
        long pow = 1L;
        for (int i = 0; i < scale; i++) {
            pow *= 10L;
        }
        return (Long.MAX_VALUE - (pow - 1)) / pow;
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

    private static ValueConverter buildArrayConverter(LogicalType flinkType, ClickHouseColumn target,
                                                      ZoneId zone, String path) {
        requireTargetType(target, ClickHouseDataType.Array, flinkType, "Array(T)");
        LogicalType elementType = ((ArrayType) flinkType).getElementType();
        ClickHouseColumn elementColumn = target.getNestedColumns().get(0);
        checkArrayElementNullability(elementType, elementColumn);

        ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        ValueConverter elementConverter = buildNestedConverter(
                elementType, elementColumn, zone, path + " element", "array element");
        return value -> toPayloadList((ArrayData) value, elementGetter, elementConverter);
    }

    /** {@code Array(Nullable(T))} is the one nested shape that can carry nulls. */
    private static void checkArrayElementNullability(LogicalType elementType,
                                                     ClickHouseColumn elementColumn) {
        if (elementType.isNullable() && !elementColumn.isNullable()) {
            throw TypeMappingException.mismatch(String.format(
                    "the Flink array element type %s is nullable but the ClickHouse element type %s "
                    + "is not Nullable — declare the element NOT NULL or make the element Nullable",
                    elementType.asSummaryString(), elementColumn.getOriginalTypeName()));
        }
    }

    private static ValueConverter buildMapConverter(LogicalType flinkType, ClickHouseColumn target,
                                                    ZoneId zone, String path) {
        requireTargetType(target, ClickHouseDataType.Map, flinkType, "Map(K, V)");
        MapType mapType = (MapType) flinkType;
        LogicalType keyType = mapType.getKeyType();
        LogicalType valueType = mapType.getValueType();
        ValueConverter keyConverter = buildMapKeyConverter(keyType, target.getKeyInfo(), zone, path);
        ValueConverter valueConverter = buildMapValueConverter(valueType, target.getValueInfo(), zone, path);
        return buildPayloadMapConverter(keyType, keyConverter, valueType, valueConverter, path);
    }

    /** MULTISET&lt;T&gt; is a map from T to a non-null int count, matched against {@code Map(T', UInt64)}. */
    private static ValueConverter buildMultisetConverter(LogicalType flinkType, ClickHouseColumn target,
                                                         ZoneId zone, String path) {
        requireTargetType(target, ClickHouseDataType.Map, flinkType, "Map(T, UInt64)");
        checkMultisetCountTarget(target);
        LogicalType elementType = ((MultisetType) flinkType).getElementType();
        LogicalType countType = new IntType(false);
        ValueConverter keyConverter = buildMapKeyConverter(elementType, target.getKeyInfo(), zone, path);
        // DataWriter's UInt64 write path takes a Long; the count getter yields an Integer.
        ValueConverter countConverter = buildMultisetCountConverter(path);
        return buildPayloadMapConverter(elementType, keyConverter, countType, countConverter, path);
    }

    /** Counts are non-negative by definition; a corrupt negative count must not wrap into UInt64. */
    private static ValueConverter buildMultisetCountConverter(String path) {
        return count -> {
            int value = (Integer) count;
            if (value < 0) {
                throw new IllegalArgumentException(
                        "Column '" + path + "': MULTISET count " + value
                        + " is negative and cannot be written to UInt64");
            }
            return (long) value;
        };
    }

    private static void checkMultisetCountTarget(ClickHouseColumn target) {
        ClickHouseColumn valueColumn = rejectNestedSimpleAggregateFunction(
                target.getValueInfo(), "the MULTISET count value");
        if (valueColumn.getDataType() != ClickHouseDataType.UInt64 || valueColumn.isNullable()) {
            throw TypeMappingException.mismatch(String.format(
                    "MULTISET counts require a Map value type of exactly UInt64, found %s",
                    target.getValueInfo().getOriginalTypeName()));
        }
    }

    /** Shared by MAP and MULTISET: both arrive as {@code MapData} and target a ClickHouse {@code Map}. */
    private static ValueConverter buildPayloadMapConverter(LogicalType keyType, ValueConverter keyConverter,
                                                      LogicalType valueType, ValueConverter valueConverter,
                                                      String path) {
        ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(keyType);
        ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
        return value -> toPayloadMap((MapData) value, keyGetter, valueGetter,
                keyConverter, valueConverter, path);
    }

    /**
     * Keys become the payload map's string keys, so the converter stringifies them and the
     * client's serializer parses them back.
     */
    private static ValueConverter buildMapKeyConverter(LogicalType keyType, ClickHouseColumn keyColumn,
                                                       ZoneId zone, String path) {
        checkMapKeyNullability(keyColumn);
        checkMapKeyIsRestorableFromString(keyColumn);
        ValueConverter keyConverter = buildNestedConverter(
                keyType, keyColumn, zone, path + " key", "map key");
        return value -> String.valueOf(keyConverter.convert(value));
    }

    /**
     * ClickHouse Map keys can never be Nullable. Flink SQL marks every MAP key type nullable, so
     * the Flink side is exempt and a null key value fails in {@link #toPayloadMap} instead.
     */
    private static void checkMapKeyNullability(ClickHouseColumn keyColumn) {
        if (keyColumn.isNullable()) {
            throw TypeMappingException.mismatch("ClickHouse Map keys cannot be Nullable");
        }
    }

    private static void checkMapKeyIsRestorableFromString(ClickHouseColumn keyColumn) {
        if (STRING_RESTORABLE_MAP_KEY_TARGETS.contains(keyColumn.getDataType())) {
            return;
        }
        if (keyColumn.getDataType() == ClickHouseDataType.UInt64) {
            throw TypeMappingException.mismatch(
                    "ClickHouse Map keys of type UInt64 are not supported by the sink — map keys "
                    + "are checkpointed as strings and the client serializer restores them with a "
                    + "signed-long parse, which fails for the upper half of the UInt64 range; "
                    + "use an Int64 or UInt128 key column instead");
        }
        throw TypeMappingException.mismatch(String.format(
                "ClickHouse Map key type %s is not supported by the sink — map keys are "
                + "checkpointed as strings and %s cannot be restored from a string",
                keyColumn.getOriginalTypeName(), keyColumn.getDataType()));
    }

    private static ValueConverter buildMapValueConverter(LogicalType valueType, ClickHouseColumn valueColumn,
                                                         ZoneId zone, String path) {
        checkMapValueNullability(valueType, valueColumn);
        return buildNestedConverter(valueType, valueColumn, zone, path + " value", "map value");
    }

    /**
     * Neither side may be nullable: the client's serializer never writes a Map value's
     * non-null marker, so a Nullable value type cannot be written byte-exactly.
     */
    private static void checkMapValueNullability(LogicalType valueType, ClickHouseColumn valueColumn) {
        if (valueColumn.isNullable()) {
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
    }

    /**
     * Converts every entry of a {@code MAP}/{@code MULTISET} into the string-keyed {@code Map} the
     * payload carries. Nulls have nowhere to go here, so they fail naming the column.
     */
    private static Map<String, Object> toPayloadMap(MapData map,
                                                        ArrayData.ElementGetter keyGetter,
                                                        ArrayData.ElementGetter valueGetter,
                                                        ValueConverter keyConverter,
                                                        ValueConverter valueConverter,
                                                        String path) {
        ArrayData keys = map.keyArray();
        ArrayData values = map.valueArray();
        int size = map.size();
        // initialCapacity is a bucket count, not an entry count — undershoot forces a rehash.
        Map<String, Object> result = new LinkedHashMap<>((int) (size / 0.75f) + 1);
        for (int i = 0; i < size; i++) {
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

    /** Converts every element of an {@code ARRAY} into the plain {@code List} the payload carries. */
    private static List<Object> toPayloadList(ArrayData array, ArrayData.ElementGetter getter,
                                                    ValueConverter elementConverter) {
        int size = array.size();
        List<Object> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Object element = getter.getElementOrNull(array, i);
            result.add(element == null ? null : elementConverter.convert(element));
        }
        return result;
    }

    /** ROW fields match Tuple elements positionally. */
    private static ValueConverter buildRowConverter(LogicalType flinkType, ClickHouseColumn target,
                                                    ZoneId zone, String path) {
        requireTargetType(target, ClickHouseDataType.Tuple, flinkType, "Tuple(...)");
        RowType rowType = (RowType) flinkType;
        List<ClickHouseColumn> elements = target.getNestedColumns();
        checkRowFieldCountMatchesTuple(rowType, elements);

        RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[rowType.getFieldCount()];
        ValueConverter[] fieldConverters = new ValueConverter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            RowType.RowField field = rowType.getFields().get(i);
            fieldGetters[i] = RowData.createFieldGetter(field.getType(), i);
            fieldConverters[i] = buildRowFieldConverter(field, elements.get(i), i, zone, path);
        }
        return value -> toPayloadTuple((RowData) value, fieldGetters, fieldConverters, path);
    }

    private static void checkRowFieldCountMatchesTuple(RowType rowType, List<ClickHouseColumn> elements) {
        if (rowType.getFieldCount() != elements.size()) {
            throw TypeMappingException.mismatch(String.format(
                    "ROW has %d fields but the Tuple has %d elements",
                    rowType.getFieldCount(), elements.size()));
        }
    }

    private static ValueConverter buildRowFieldConverter(RowType.RowField field, ClickHouseColumn element,
                                                         int position, ZoneId zone, String path) {
        checkRowFieldNullability(field, element, position);
        return buildNestedConverter(field.getType(), element, zone,
                path + "." + field.getName(), "ROW field '" + field.getName() + "'");
    }

    /** Neither side may be nullable, for the same serializer gap as {@link #checkMapValueNullability}. */
    private static void checkRowFieldNullability(RowType.RowField field, ClickHouseColumn element,
                                                 int position) {
        if (element.isNullable()) {
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
    }

    /** Converts every field of a {@code ROW} into the {@code Object[]} tuple the payload carries. */
    private static Object[] toPayloadTuple(RowData row, RowData.FieldGetter[] getters,
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

    /**
     * Unsigned targets reject sign/overflow per record, mirroring the DATE→Date range check:
     * the client's writer would otherwise fail without naming the column.
     */
    private static long checkUnsignedRange(long value, long max, String targetType, String path) {
        if (value < 0 || value > max) {
            throw new IllegalArgumentException(
                    "Column '" + path + "': value " + value + " is outside the "
                    + targetType + " range 0.." + max);
        }
        return value;
    }

    private static void requireTargetType(ClickHouseColumn target, ClickHouseDataType expected,
                                          LogicalType flinkType, String supportedTargets) {
        if (target.getDataType() != expected) {
            throw noConversion(flinkType, supportedTargets);
        }
    }

    /** Recurses into a nested pair, prefixing mismatch reasons with the structural context. */
    private static ValueConverter buildNestedConverter(LogicalType flinkType, ClickHouseColumn column,
                                                       ZoneId zone, String path, String context) {
        // Every composite recursion passes here, so nested SAF can never reach converterFor's unwrap.
        rejectNestedSimpleAggregateFunction(column, context);
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
