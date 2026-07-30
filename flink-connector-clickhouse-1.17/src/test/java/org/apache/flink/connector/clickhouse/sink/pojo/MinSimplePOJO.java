package org.apache.flink.connector.clickhouse.sink.pojo;

import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * {@link SimplePOJO} with every type set to its supported MINIMUM (issue #114).
 * longPrimitive = Long.MIN_VALUE (also the ORDER BY key), so it sorts before {@link MaxSimplePOJO}.
 *
 * <p>Every numeric and boolean field is set explicitly below; only the non-numeric fields
 * (string, uuid, collections) are inherited from the {@link SimplePOJO} constructor.
 * Float/Double minima use {@code -MAX_VALUE} (the most-negative value); {@code MIN_VALUE}
 * would be the smallest POSITIVE value, not a lower bound.
 *
 * <pre>
 * | ClickHouse type | Min                   | How derived                     |
 * |-----------------|-----------------------|---------------------------------|
 * | Int8            | Byte.MIN_VALUE (-128) | JDK constant                    |
 * | Int16           | Short.MIN_VALUE       | JDK constant                    |
 * | Int32           | Integer.MIN_VALUE     | JDK constant                    |
 * | Int64           | Long.MIN_VALUE        | JDK constant; also ORDER BY key |
 * | Int128          | -2^127                | min signed 16-byte int          |
 * | Int256          | -2^255                | min signed 32-byte int          |
 * | UInt8           | 0                     | unsigned min                    |
 * | UInt16          | 0                     | unsigned min                    |
 * | UInt32          | 0                     | unsigned min                    |
 * | UInt64          | 0                     | unsigned min                    |
 * | UInt128         | 0                     | unsigned min                    |
 * | UInt256         | 0                     | unsigned min                    |
 * | Float32         | -Float.MAX_VALUE      | most negative float             |
 * | Float64         | -Double.MAX_VALUE     | most negative double            |
 * | Bool            | false                 | the smaller of the two values   |
 * | Decimal(10,5)   | -99999.99999          | -(10^P - 1) / 10^S, P=10 S=5    |
 * | Decimal32(9)    | -0.999999999          | -(10^P - 1) / 10^S, P=9  S=9    |
 * | Decimal64(18)   | -0.(18 nines)         | -(10^P - 1) / 10^S, P=18 S=18   |
 * | Decimal128(38)  | -0.(38 nines)         | -(10^P - 1) / 10^S, P=38 S=38   |
 * | Decimal256(76)  | -0.(76 nines)         | -(10^P - 1) / 10^S, P=76 S=76   |
 * | Date            | 1970-01-01            | CH Date lower bound (epoch)     |
 * | Date32          | 1900-01-01            | CH Date32 lower bound           |
 * | DateTime        | 1970-01-01 00:00:00   | CH DateTime lower bound (epoch) |
 * | DateTime64(6)   | 1900-01-01 00:00:00   | CH DateTime64 min on 24.3       |
 * </pre>
 *
 * <p>Note: Date32 and DateTime64 ranges are ClickHouse-version-dependent. The values above
 * match the 24.3 image the tests run against (DateTime64 clamps to [1900-01-01, 2299-12-31]);
 * newer ClickHouse widens DateTime64 to [0000-01-01, 9999-12-31] for precision &lt;= 7.
 */
public class MinSimplePOJO extends SimplePOJO {

    public MinSimplePOJO() {
        super(0);
        setBytePrimitive(Byte.MIN_VALUE);
        setByteObject(Byte.MIN_VALUE);
        setShortPrimitive(Short.MIN_VALUE);
        setShortObject(Short.MIN_VALUE);
        setIntPrimitive(Integer.MIN_VALUE);
        setIntegerObject(Integer.MIN_VALUE);
        setLongPrimitive(Long.MIN_VALUE); // Int64 min; also the ORDER BY key (sorts before MaxSimplePOJO)
        setLongObject(Long.MIN_VALUE);
        setUint8PrimitiveInt(0);
        setUint8ObjectInt(0);
        setUint8PrimitiveShort((short) 0);
        setUint8ObjectShort((short) 0);
        setUint16Primitive(0);
        setUint16Object(0);
        setUint32Primitive(0L);
        setUint32Object(0L);
        setUint64PrimitiveLong(0L);
        setUint64ObjectLong(0L);
        setUint64ObjectBigInt(BigInteger.ZERO);
        setBigInteger128(BigInteger.ONE.shiftLeft(127).negate());   // Int128 min = -2^127
        setBigInteger256(BigInteger.ONE.shiftLeft(255).negate());   // Int256 min = -2^255
        setUint128Object(BigInteger.ZERO);
        setUint256Object(BigInteger.ZERO);
        setBigDecimal(MaxSimplePOJO.maxDecimal(10, 5).negate());
        setBigDecimal32(MaxSimplePOJO.maxDecimal(9, 9).negate());
        setBigDecimal64(MaxSimplePOJO.maxDecimal(18, 18).negate());
        setBigDecimal128(MaxSimplePOJO.maxDecimal(38, 38).negate());
        setBigDecimal256(MaxSimplePOJO.maxDecimal(76, 76).negate());
        setFloatPrimitive(-Float.MAX_VALUE);
        setFloatObject(-Float.MAX_VALUE);
        setDoublePrimitive(-Double.MAX_VALUE);
        setDoubleObject(-Double.MAX_VALUE);
        setBooleanPrimitive(false);
        setBooleanObject(false);
        setDateObject(LocalDate.of(1970, 1, 1));
        setDate32Object(LocalDate.of(1900, 1, 1));
        setDateTimeObjectLocal(LocalDateTime.of(1970, 1, 1, 0, 0, 0));
        setDateTimeObjectZoned(ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC")));
        setDateTime64ObjectLocal(LocalDateTime.of(1900, 1, 1, 0, 0, 0));
        setDateTime64ObjectZoned(ZonedDateTime.of(1900, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC")));
    }
}
