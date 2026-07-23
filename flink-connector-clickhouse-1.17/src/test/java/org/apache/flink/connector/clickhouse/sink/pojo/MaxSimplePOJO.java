package org.apache.flink.connector.clickhouse.sink.pojo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * {@link SimplePOJO} with every type set to its supported MAXIMUM (issue #114).
 * longPrimitive = Long.MAX_VALUE (also the ORDER BY key), so it sorts after {@link MinSimplePOJO}.
 *
 * <p>Signed 8/16/32-bit and float/double maxima are inherited from the {@link SimplePOJO}
 * constructor; the fields below are the ones that needed true extremes.
 *
 * <pre>
 * | ClickHouse type | Max                        | How derived                          |
 * |-----------------|----------------------------|--------------------------------------|
 * | Int8            | Byte.MAX_VALUE (127)       | JDK constant (ctor)                  |
 * | Int16           | Short.MAX_VALUE            | JDK constant (ctor)                  |
 * | Int32           | Integer.MAX_VALUE          | JDK constant (ctor)                  |
 * | Int64           | Long.MAX_VALUE             | JDK constant; also ORDER BY key      |
 * | Int128          | 2^127 - 1                  | max signed 16-byte int               |
 * | Int256          | 2^255 - 1                  | max signed 32-byte int               |
 * | UInt8           | 255                        | max unsigned 1-byte int              |
 * | UInt16          | 65535                      | max unsigned 2-byte int              |
 * | UInt32          | 4294967295                 | max unsigned 4-byte int (2^32-1)     |
 * | UInt64          | 2^64 - 1                   | max unsigned 8-byte int              |
 * | UInt128         | 2^128 - 1                  | max unsigned 16-byte int             |
 * | UInt256         | 2^256 - 1                  | max unsigned 32-byte int             |
 * | Float32         | Float.MAX_VALUE            | JDK constant (ctor)                  |
 * | Float64         | Double.MAX_VALUE           | JDK constant (ctor)                  |
 * | Bool            | true                       | the larger of the two values         |
 * | Decimal(10,5)   | 99999.99999                | (10^P - 1) / 10^S, P=10 S=5          |
 * | Decimal32(9)    | 0.999999999                | (10^P - 1) / 10^S, P=9  S=9          |
 * | Decimal64(18)   | 0.(18 nines)               | (10^P - 1) / 10^S, P=18 S=18         |
 * | Decimal128(38)  | 0.(38 nines)               | (10^P - 1) / 10^S, P=38 S=38         |
 * | Decimal256(76)  | 0.(76 nines)               | (10^P - 1) / 10^S, P=76 S=76         |
 * | Date            | 2149-06-06                 | CH Date upper bound (UInt16 days)    |
 * | Date32          | 2299-12-31                 | CH Date32 upper bound                |
 * | DateTime        | 2106-02-07 06:28:15        | CH DateTime upper bound (UInt32 sec) |
 * | DateTime64(6)   | 2299-12-31 23:59:59.999999 | CH DateTime64 max on 24.3            |
 * </pre>
 *
 * <p>Note: Date32 and DateTime64 ranges are ClickHouse-version-dependent. The values above
 * match the 24.3 image the tests run against (DateTime64 clamps to [1900-01-01, 2299-12-31]);
 * newer ClickHouse widens DateTime64 to [0000-01-01, 9999-12-31] for precision &lt;= 7.
 */
public class MaxSimplePOJO extends SimplePOJO {

    public MaxSimplePOJO() {
        super(1);
        setLongPrimitive(Long.MAX_VALUE); // Int64 max; also the ORDER BY key (sorts after MinSimplePOJO)
        // ctor seeds these at signed maxima; override with the true unsigned maxima
        setUint8PrimitiveInt(255);
        setUint8ObjectInt(255);
        setUint8PrimitiveShort((short) 255);
        setUint8ObjectShort((short) 255);
        setUint16Primitive(65535);
        setUint16Object(65535);
        setUint32Primitive(4294967295L);
        setUint32Object(4294967295L);
        setUint64ObjectBigInt(BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE));   // 2^64 - 1
        setUint128Object(BigInteger.ONE.shiftLeft(128).subtract(BigInteger.ONE));       // 2^128 - 1
        setUint256Object(BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE));       // 2^256 - 1
        setBigInteger128(BigInteger.ONE.shiftLeft(127).subtract(BigInteger.ONE));       // Int128 max
        setBigInteger256(BigInteger.ONE.shiftLeft(255).subtract(BigInteger.ONE));       // Int256 max
        setBigDecimal(maxDecimal(10, 5));
        setBigDecimal32(maxDecimal(9, 9));
        setBigDecimal64(maxDecimal(18, 18));
        setBigDecimal128(maxDecimal(38, 38));
        setBigDecimal256(maxDecimal(76, 76));
        setDateObject(LocalDate.of(2149, 6, 6));
        setDate32Object(LocalDate.of(2299, 12, 31));
        setDateTimeObjectLocal(LocalDateTime.of(2106, 2, 7, 6, 28, 15));
        setDateTimeObjectZoned(ZonedDateTime.of(2106, 2, 7, 6, 28, 15, 0, ZoneId.of("UTC")));
        // scale 6 => microseconds; 999_999_000 ns = .999999 us (the exact DateTime64 max on CH 24.3)
        setDateTime64ObjectLocal(LocalDateTime.of(2299, 12, 31, 23, 59, 59, 999_999_000));
        setDateTime64ObjectZoned(ZonedDateTime.of(2299, 12, 31, 23, 59, 59, 999_999_000, ZoneId.of("UTC")));
    }

    /** Largest unscaled value representable at {@code precision}, at the given {@code scale}. */
    static BigDecimal maxDecimal(int precision, int scale) {
        return new BigDecimal(BigInteger.TEN.pow(precision).subtract(BigInteger.ONE), scale);
    }
}
