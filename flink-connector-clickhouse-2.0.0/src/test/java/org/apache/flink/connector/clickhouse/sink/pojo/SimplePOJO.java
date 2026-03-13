package org.apache.flink.connector.clickhouse.sink.pojo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

/**
 * For testing most data types.
 * Follows the rules specified in DefaultColumnToMethodMatchingStrategy.java to allow for the java client to deserialize it.
 */
public class SimplePOJO {
    private byte bytePrimitive; // Int8
    private Byte byteObject; // Int8

    private short shortPrimitive; // Int16
    private Short shortObject; // Int16

    private int intPrimitive; // Int32
    private Integer integerObject; // Int32

    private long longPrimitive; // Int64
    private Long longObject; // Int64

    private int uint8PrimitiveInt; // UInt8
    private Integer uint8ObjectInt; // UInt8

    private short uint8PrimitiveShort; // UInt8
    private Short uint8ObjectShort; // UInt8

    private int uint16Primitive; // UInt16
    private Integer uint16Object; // UInt16

    private long uint32Primitive; // UInt32
    private Long uint32Object; // UInt32

    private long uint64PrimitiveLong; // UInt64
    private Long uint64ObjectLong; // UInt64

    private BigInteger uint64ObjectBigInt; // UInt64

    private BigInteger bigInteger128; // Int128
    private BigInteger bigInteger256; // Int256

    private BigInteger uint128Object; // UInt128
    private BigInteger uint256Object; // UInt256

    private BigDecimal bigDecimal; // Decimal(10,5)
    private BigDecimal bigDecimal32; // Decimal32(9)
    private BigDecimal bigDecimal64; // Decimal64(18)
    private BigDecimal bigDecimal128; // Decimal128(38)
    private BigDecimal bigDecimal256; // Decimal256(76)

    private float floatPrimitive; // Float
    private Float floatObject; // Float

    private double doublePrimitive; // Double
    private Double doubleObject; // Double

    private boolean booleanPrimitive; // Bool
    private Boolean booleanObject; // Bool

    private String str; // String

    private String fixedStr; // FixedString(10)

    private LocalDate date; // Date
    private LocalDate date32; // Date32

    private LocalDateTime localDateTime; // DateTime
    private LocalDateTime localDateTime64; // DateTime64

    private ZonedDateTime zonedDateTime; // DateTime
    private ZonedDateTime zonedDateTime64; // DateTime64

    private UUID uuid; // UUID

    private List<String> stringList; // Array(String)
    private List<Long> longList; // Array(Int64)

    private Map<String, String> mapOfStrings; // Map(String,String)

    private List<Object> tupleOfObjects; // Tuple(String,Int64,Boolean)

    public SimplePOJO() {
    }

    public SimplePOJO(int index) {
        this.bytePrimitive = Byte.MIN_VALUE;
        this.byteObject = Byte.MAX_VALUE;

        this.shortPrimitive = Short.MIN_VALUE;
        this.shortObject = Short.MAX_VALUE;

        this.intPrimitive = Integer.MIN_VALUE;
        this.integerObject = Integer.MAX_VALUE;

        this.longPrimitive = index;
        this.longObject = Long.MAX_VALUE;

        this.uint8PrimitiveInt = Byte.MAX_VALUE;
        this.uint8ObjectInt = (int) Byte.MAX_VALUE;

        this.uint8PrimitiveShort = Byte.MAX_VALUE;
        this.uint8ObjectShort = (short) Byte.MAX_VALUE;

        this.uint16Primitive = Short.MAX_VALUE;
        this.uint16Object = (int) Short.MAX_VALUE;

        this.uint32Primitive = Integer.MAX_VALUE;
        this.uint32Object = (long) Integer.MAX_VALUE;

        this.uint64PrimitiveLong = Long.MAX_VALUE;
        this.uint64ObjectLong = Long.MAX_VALUE;

        this.uint64ObjectBigInt = BigInteger.valueOf(Long.MAX_VALUE);

        this.uint128Object = BigInteger.valueOf(index);
        this.uint256Object = BigInteger.valueOf(index);

        this.bigDecimal = BigDecimal.valueOf(index, 5); // scale = 5
        this.bigDecimal32 = BigDecimal.valueOf(index, 9); // scale = 9
        this.bigDecimal64 = BigDecimal.valueOf(index, 18); // scale = 18
        this.bigDecimal128 = BigDecimal.valueOf(index, 38); // scale = 38
        this.bigDecimal256 = BigDecimal.valueOf(index, 76); // scale = 76

        this.floatPrimitive = Float.MIN_VALUE;
        this.floatObject = Float.MAX_VALUE;

        this.doublePrimitive = Double.MIN_VALUE;
        this.doubleObject = Double.MAX_VALUE;

        this.booleanPrimitive = true;
        this.booleanObject = Boolean.FALSE;

        this.str = "str" + longPrimitive;
        this.fixedStr = (str + "_FixedString").substring(0, 10);

        this.bigInteger128 = BigInteger.valueOf(longPrimitive);
        this.bigInteger256 = BigInteger.valueOf(longPrimitive);

        this.date = LocalDate.ofEpochDay(0);
        this.date32 = LocalDate.ofEpochDay(0);

        this.localDateTime = ZonedDateTime.of(LocalDateTime.of(2026, 2, 11, 12, 45, 59), ZoneId.of("UTC")).toLocalDateTime();
        this.localDateTime64 = ZonedDateTime.of(LocalDateTime.now(), ZoneId.of("UTC")).toLocalDateTime();

        this.zonedDateTime = ZonedDateTime.of(LocalDateTime.of(2026, 2, 11, 12, 45, 59), ZoneId.of("UTC"));
        this.zonedDateTime64 = ZonedDateTime.of(LocalDateTime.now(), ZoneId.of("UTC"));

        this.uuid = UUID.randomUUID();

        this.stringList = new ArrayList<>();
        this.stringList.add("a");
        this.stringList.add("b");
        this.stringList.add("c");
        this.stringList.add("d");

        this.longList = new ArrayList<>();
        this.longList.add(1L);
        this.longList.add(2L);
        this.longList.add(3L);
        this.longList.add(4L);

        this.mapOfStrings = new HashMap<>();
        this.mapOfStrings.put("a", "a");
        this.mapOfStrings.put("b", "b");

        this.tupleOfObjects = new ArrayList<>();
        this.tupleOfObjects.add("test");
        this.tupleOfObjects.add(1L);
        this.tupleOfObjects.add(true);

    }

    // getters
    public byte getBytePrimitive() {
        return bytePrimitive;
    }

    public Byte getByteObject() {
        return byteObject;
    }

    public short getShortPrimitive() {
        return shortPrimitive;
    }

    public Short getShortObject() {
        return shortObject;
    }

    public int getIntPrimitive() {
        return intPrimitive;
    }

    public Integer getIntegerObject() {
        return integerObject;
    }

    public long getLongPrimitive() {
        return longPrimitive;
    }

    public Long getLongObject() {
        return longObject;
    }

    public BigInteger getBigInteger128() {
        return bigInteger128;
    }

    public BigInteger getBigInteger256() {
        return bigInteger256;
    }

    public int getUint8PrimitiveInt() {
        return uint8PrimitiveInt;
    }

    public Integer getUint8ObjectInt() {
        return uint8ObjectInt;
    }

    public short getUint8PrimitiveShort() {
        return uint8PrimitiveShort;
    }

    public Short getUint8ObjectShort() {
        return uint8ObjectShort;
    }

    public int getUint16Primitive() {
        return uint16Primitive;
    }

    public Integer getUint16Object() {
        return uint16Object;
    }

    public long getUint32Primitive() {
        return uint32Primitive;
    }

    public Long getUint32Object() {
        return uint32Object;
    }

    public long getUint64PrimitiveLong() {
        return uint64PrimitiveLong;
    }

    public Long getUint64ObjectLong() {
        return uint64ObjectLong;
    }

    public BigInteger getUint64ObjectBigInt() {
        return uint64ObjectBigInt;
    }

    public BigInteger getUint128Object() {
        return uint128Object;
    }

    public BigInteger getUint256Object() {
        return uint256Object;
    }

    public BigDecimal getBigDecimal() {
        return bigDecimal;
    }

    public BigDecimal getBigDecimal32() {
        return bigDecimal32;
    }

    public BigDecimal getBigDecimal64() {
        return bigDecimal64;
    }

    public BigDecimal getBigDecimal128() {
        return bigDecimal128;
    }

    public BigDecimal getBigDecimal256() {
        return bigDecimal256;
    }

    public float getFloatPrimitive() {
        return floatPrimitive;
    }

    public Float getFloatObject() {
        return floatObject;
    }

    public double getDoublePrimitive() {
        return doublePrimitive;
    }

    public Double getDoubleObject() {
        return doubleObject;
    }

    public boolean getBooleanPrimitive() {
        return booleanPrimitive;
    }

    public Boolean getBooleanObject() {
        return booleanObject;
    }

    public String getStr() {
        return str;
    }

    public String getFixedStr() {
        return fixedStr;
    }

    public LocalDate getDateObject() {
        return date;
    }

    public LocalDate getDate32Object() {
        return date32;
    }

    public LocalDateTime getDateTimeObjectLocal() {
        return localDateTime;
    }

    public LocalDateTime getDateTime64ObjectLocal() {
        return localDateTime64;
    }

    public ZonedDateTime getDateTimeObjectZoned() {
        return zonedDateTime;
    }

    public ZonedDateTime getDateTime64ObjectZoned() {
        return zonedDateTime64;
    }

    public UUID getUuid() {
        return uuid;
    }

    public List<String> getStringList() {
        return stringList;
    }

    public List<Long> getLongList() {
        return longList;
    }

    public Map<String, String> getMapOfStrings() {
        return mapOfStrings;
    }

    public List<Object> getTupleOfObjects() {
        return tupleOfObjects;
    }

    // setters
    public void setBytePrimitive(byte bytePrimitive) {
        this.bytePrimitive = bytePrimitive;
    }

    public void setByteObject(Byte byteObject) {
        this.byteObject = byteObject;
    }

    public void setShortPrimitive(short shortPrimitive) {
        this.shortPrimitive = shortPrimitive;
    }

    public void setShortObject(Short shortObject) {
        this.shortObject = shortObject;
    }

    public void setIntPrimitive(int intPrimitive) {
        this.intPrimitive = intPrimitive;
    }

    public void setIntegerObject(Integer integerObject) {
        this.integerObject = integerObject;
    }

    public void setLongPrimitive(long longPrimitive) {
        this.longPrimitive = longPrimitive;
    }

    public void setLongObject(Long longObject) {
        this.longObject = longObject;
    }

    public void setBigInteger128(BigInteger bigInteger128) {
        this.bigInteger128 = bigInteger128;
    }

    public void setBigInteger256(BigInteger bigInteger256) {
        this.bigInteger256 = bigInteger256;
    }

    public void setUint8PrimitiveInt(int uint8Primitive) {
        this.uint8PrimitiveInt = uint8Primitive;
    }

    public void setUint8ObjectInt(Object uint8Object) {
        if (uint8Object instanceof Short) {
            this.uint8ObjectInt = ((Short) uint8Object).intValue();
        } else {
            this.uint8ObjectInt = (Integer) uint8Object;
        }
    }

    public void setUint8PrimitiveShort(short uint8Primitive) {
        this.uint8PrimitiveShort = uint8Primitive;
    }

    public void setUint8ObjectShort(Short uint8Object) {
        this.uint8ObjectShort = uint8Object;
    }

    public void setUint16Primitive(int uint16Primitive) {
        this.uint16Primitive = uint16Primitive;
    }

    public void setUint16Object(Integer uint16Object) {
        this.uint16Object = uint16Object;
    }

    public void setUint32Primitive(long uint32Primitive) {
        this.uint32Primitive = uint32Primitive;
    }

    public void setUint32Object(Long uint32Object) {
        this.uint32Object = uint32Object;
    }

    public void setUint64PrimitiveLong(Object uint64Primitive) {
        if (uint64Primitive instanceof BigInteger) {
            this.uint64PrimitiveLong = ((BigInteger) uint64Primitive).longValue();
        } else {
            this.uint64PrimitiveLong = (long) uint64Primitive;
        }
    }

    public void setUint64ObjectLong(Object uint64Object) {
        if (uint64Object instanceof BigInteger) {
            this.uint64ObjectLong = ((BigInteger) uint64Object).longValue();
        } else {
            this.uint64ObjectLong = (Long) uint64Object;
        }
    }

    public void setUint64ObjectBigInt(BigInteger uint64ObjectBigInt) {
        this.uint64ObjectBigInt = uint64ObjectBigInt;
    }

    public void setUint128Object(BigInteger uint128Object) {
        this.uint128Object = uint128Object;
    }

    public void setUint256Object(BigInteger uint256Object) {
        this.uint256Object = uint256Object;
    }

    public void setBigDecimal(BigDecimal bigDecimal) {
        this.bigDecimal = bigDecimal;
    }

    public void setBigDecimal32(BigDecimal bigDecimal32) {
        this.bigDecimal32 = bigDecimal32;
    }

    public void setBigDecimal64(BigDecimal bigDecimal64) {
        this.bigDecimal64 = bigDecimal64;
    }

    public void setBigDecimal128(BigDecimal bigDecimal128) {
        this.bigDecimal128 = bigDecimal128;
    }

    public void setBigDecimal256(BigDecimal bigDecimal256) {
        this.bigDecimal256 = bigDecimal256;
    }

    public void setFloatPrimitive(float floatPrimitive) {
        this.floatPrimitive = floatPrimitive;
    }

    public void setFloatObject(Float floatObject) {
        this.floatObject = floatObject;
    }

    public void setDoublePrimitive(double doublePrimitive) {
        this.doublePrimitive = doublePrimitive;
    }

    public void setDoubleObject(Double doubleObject) {
        this.doubleObject = doubleObject;
    }

    public void setBooleanPrimitive(boolean booleanPrimitive) {
        this.booleanPrimitive = booleanPrimitive;
    }

    public void setBooleanObject(Boolean booleanObject) {
        this.booleanObject = booleanObject;
    }

    public void setStr(String str) {
        this.str = str;
    }

    public void setFixedStr(String fixedStr) {
        this.fixedStr = fixedStr;
    }

    public void setDateObject(LocalDate date) {
        this.date = date;
    }

    public void setDate32Object(LocalDate date32) {
        this.date32 = date32;
    }

    public void setDateTimeObjectLocal(Object dateTimeObject) {
        if (dateTimeObject instanceof ZonedDateTime) {
            ZoneId utcZone = ZoneId.of("UTC");
            this.localDateTime = ((ZonedDateTime) dateTimeObject).withZoneSameInstant(utcZone).toLocalDateTime();
        } else {
            this.localDateTime = (LocalDateTime) dateTimeObject;
        }
    }

    public void setDateTime64ObjectLocal(Object dateTime64Object) {
        if (dateTime64Object instanceof ZonedDateTime) {
            ZoneId utcZone = ZoneId.of("UTC");
            this.localDateTime64 = ((ZonedDateTime) dateTime64Object).withZoneSameInstant(utcZone).toLocalDateTime();
        } else {
            this.localDateTime64 = (LocalDateTime) dateTime64Object;
        }
    }

    public void setDateTimeObjectZoned(ZonedDateTime dateTimeZoned) {
        this.zonedDateTime = dateTimeZoned;
    }

    public void setDateTime64ObjectZoned(ZonedDateTime dateTime64Zoned) {
        this.zonedDateTime64 = dateTime64Zoned;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    public void setStringList(List<String> stringList) {
        this.stringList = stringList;
    }

    public void setLongList(List<Long> longList) {
        this.longList = longList;
    }

    public void setMapOfStrings(Map<String, String> mapOfStrings) {
        this.mapOfStrings = mapOfStrings;
    }

    public void setTupleOfObjects(List<Object> tupleOfObjects) {
        this.tupleOfObjects = tupleOfObjects;
    }

    public static String createTableSQL(String database, String tableName, int partsToThrowInsert) {
        String createTable = createTableSQL(database, tableName);
        return createTable.trim().substring(0, createTable.trim().length() - 1) + " " + String.format("SETTINGS parts_to_throw_insert = %d;", partsToThrowInsert);
    }

    public static String createTableSQLWithDedupWindow(String database, String tableName, int dedupWindow) {
        String createTable = createTableSQL(database, tableName);
        return createTable.trim().substring(0, createTable.trim().length() - 1)
                + " " + String.format("SETTINGS non_replicated_deduplication_window = %d;", dedupWindow);
    }

    public static String createTableSQL(String database, String tableName) {
        return "CREATE TABLE `" + database + "`.`" + tableName + "` (" +
                "bytePrimitive Int8," +
                "byteObject Int8," +
                "shortPrimitive Int16," +
                "shortObject Int16," +
                "intPrimitive Int32," +
                "integerObject Int32," +
                "longPrimitive Int64," +
                "longObject Int64," +
                "bigInteger128 Int128," +
                "bigInteger256 Int256," +
                "uint8PrimitiveInt  UInt8," +
                "uint8ObjectInt UInt8," +
                "uint8PrimitiveShort  UInt8," +
                "uint8ObjectShort UInt8," +
                "uint16Primitive  UInt16," +
                "uint16Object UInt16," +
                "uint32Primitive  UInt32," +
                "uint32Object UInt32," +
                "uint64PrimitiveLong UInt64," +
                "uint64ObjectLong UInt64," +
                "uint64ObjectBigInt UInt64," +
                "uint128Object UInt128," +
                "uint256Object UInt256," +
                "bigDecimal Decimal(10,5)," +
                "bigDecimal32 Decimal32(9)," +
                "bigDecimal64 Decimal64(18)," +
                "bigDecimal128 Decimal128(38)," +
                "bigDecimal256 Decimal256(76)," +
                "floatPrimitive Float," +
                "floatObject Float," +
                "doublePrimitive Double," +
                "doubleObject Double," +
                "booleanPrimitive Boolean," +
                "booleanObject Boolean," +
                "str String," +
                "fixedStr FixedString(10)," +
                "dateObject Date," +
                "date32Object Date32," +
                "dateTimeObjectLocal DateTime," +
                "dateTime64ObjectLocal DateTime64(6, 'UTC')," +
                "dateTimeObjectZoned DateTime," +
                "dateTime64ObjectZoned DateTime64(6, 'UTC')," +
                "uuid UUID," +
                "stringList Array(String)," +
                "longList Array(Int64)," +
                "mapOfStrings Map(String,String)," +
                "tupleOfObjects Tuple(String,Int64,Boolean)," +
                ") " +
                "ENGINE = MergeTree " +
                "ORDER BY (longPrimitive); ";
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SimplePOJO)) {
            return false;
        }
        SimplePOJO other = (SimplePOJO) obj;
        return Objects.equals(bytePrimitive, other.bytePrimitive) &&
                Objects.equals(byteObject, other.byteObject) &&
                Objects.equals(shortPrimitive, other.shortPrimitive) &&
                Objects.equals(shortObject, other.shortObject) &&
                Objects.equals(intPrimitive, other.intPrimitive) &&
                Objects.equals(integerObject, other.integerObject) &&
                Objects.equals(longPrimitive, other.longPrimitive) &&
                Objects.equals(longObject, other.longObject) &&
                Objects.equals(bigInteger128, other.bigInteger128) &&
                Objects.equals(bigInteger256, other.bigInteger256) &&
                Objects.equals(uint8PrimitiveInt, other.uint8PrimitiveInt) &&
                Objects.equals(uint8ObjectInt, other.uint8ObjectInt) &&
                Objects.equals(uint8PrimitiveShort, other.uint8PrimitiveShort) &&
                Objects.equals(uint8ObjectShort, other.uint8ObjectShort) &&
                Objects.equals(uint16Primitive, other.uint16Primitive) &&
                Objects.equals(uint16Object, other.uint16Object) &&
                Objects.equals(uint32Primitive, other.uint32Primitive) &&
                Objects.equals(uint32Object, other.uint32Object) &&
                Objects.equals(uint64PrimitiveLong, other.uint64PrimitiveLong) &&
                Objects.equals(uint64ObjectLong, other.uint64ObjectLong) &&
                Objects.equals(uint64ObjectBigInt, other.uint64ObjectBigInt) &&
                Objects.equals(uint128Object, other.uint128Object) &&
                Objects.equals(uint256Object, other.uint256Object) &&
                Objects.equals(bigDecimal, other.bigDecimal) &&
                Objects.equals(bigDecimal32, other.bigDecimal32) &&
                Objects.equals(bigDecimal64, other.bigDecimal64) &&
                Objects.equals(bigDecimal128, other.bigDecimal128) &&
                Objects.equals(bigDecimal256, other.bigDecimal256) &&
                Objects.equals(floatPrimitive, other.floatPrimitive) &&
                Objects.equals(floatObject, other.floatObject) &&
                Objects.equals(doublePrimitive, other.doublePrimitive) &&
                Objects.equals(doubleObject, other.doubleObject) &&
                Objects.equals(booleanPrimitive, other.booleanPrimitive) &&
                Objects.equals(booleanObject, other.booleanObject) &&
                Objects.equals(str, other.str) &&
                Objects.equals(fixedStr, other.fixedStr) &&
                Objects.equals(date, other.date) &&
                Objects.equals(date32, other.date32) &&
                Objects.equals(localDateTime, other.localDateTime) &&
                Objects.equals(localDateTime64, other.localDateTime64) &&
                Objects.equals(zonedDateTime, other.zonedDateTime) &&
                Objects.equals(zonedDateTime64, other.zonedDateTime64) &&
                Objects.equals(uuid, other.uuid) &&
                Objects.equals(stringList, other.stringList) &&
                Objects.equals(longList, other.longList) &&
                Objects.equals(mapOfStrings, other.mapOfStrings) &&
                Objects.equals(tupleOfObjects, other.tupleOfObjects);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bytePrimitive, byteObject, shortPrimitive, shortObject, intPrimitive, integerObject, longPrimitive, longObject, bigInteger128, bigInteger256, uint8PrimitiveInt, uint8ObjectInt, uint8PrimitiveShort, uint8ObjectShort, uint16Primitive, uint16Object, uint32Primitive, uint32Object, uint64PrimitiveLong, uint64ObjectLong, uint64ObjectBigInt, uint128Object, uint256Object, bigDecimal, bigDecimal32, bigDecimal64, bigDecimal128, bigDecimal256, floatPrimitive, floatObject, doublePrimitive, doubleObject, booleanPrimitive, booleanObject, str, fixedStr, date, date32, localDateTime, localDateTime64, zonedDateTime, zonedDateTime64, uuid, stringList, longList, mapOfStrings, tupleOfObjects);
    }
}
