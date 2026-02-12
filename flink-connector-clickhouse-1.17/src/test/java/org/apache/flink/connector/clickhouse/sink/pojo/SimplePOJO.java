package org.apache.flink.connector.clickhouse.sink.pojo;

import scala.tools.nsc.doc.html.HtmlTags;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.*;
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

    private int uint8Primitive_Int; // UInt8
    private Integer uint8Object_Int; // UInt8

    private short uint8Primitive_Short; // UInt8
    private Short uint8Object_Short; // UInt8

    private int uint16Primitive; // UInt16
    private Integer uint16Object; // UInt16

    private long uint32Primitive; // UInt32
    private Long uint32Object; // UInt32

    private long uint64Primitive_Long; // UInt64
    private Long uint64Object_Long; // UInt64

    private BigInteger uint64Object_BigInt; // UInt64

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

    private LocalDateTime dateTime_Local; // DateTime
    private LocalDateTime dateTime64_Local; // DateTime64

    private ZonedDateTime dateTime_Zoned; // DateTime
    private ZonedDateTime dateTime64_Zoned; // DateTime64

    private UUID uuid; // UUID

    private List<String> stringList; // Array(String)
    private List<Long> longList; // Array(Int64)

    private Map<String, String> mapOfStrings; // Map(String,String)

    private List<Object> tupleOfObjects; // Tuple(String,Int64,Boolean)

    public SimplePOJO() {}

    public SimplePOJO(int index) {
        this.bytePrimitive = Byte.MIN_VALUE;
        this.byteObject = Byte.MAX_VALUE;

        this.shortPrimitive = Short.MIN_VALUE;
        this.shortObject = Short.MAX_VALUE;

        this.intPrimitive = Integer.MIN_VALUE;
        this.integerObject = Integer.MAX_VALUE;

        this.longPrimitive = index;
        this.longObject = Long.MAX_VALUE;

        this.uint8Primitive_Int = Byte.MAX_VALUE;
        this.uint8Object_Int = (int)Byte.MAX_VALUE;

        this.uint8Primitive_Short = Byte.MAX_VALUE;
        this.uint8Object_Short = (short)Byte.MAX_VALUE;

        this.uint16Primitive = Short.MAX_VALUE;
        this.uint16Object = (int)Short.MAX_VALUE;

        this.uint32Primitive = Integer.MAX_VALUE;
        this.uint32Object = (long)Integer.MAX_VALUE;

        this.uint64Primitive_Long = Long.MAX_VALUE;
        this.uint64Object_Long = Long.MAX_VALUE;

        this.uint64Object_BigInt = BigInteger.valueOf(Long.MAX_VALUE);

        this.uint128Object = BigInteger.valueOf(index);
        this.uint256Object = BigInteger.valueOf(index);

        this.bigDecimal = BigDecimal.valueOf(index, 5);
        this.bigDecimal32 = BigDecimal.valueOf(index, 9);
        this.bigDecimal64 = BigDecimal.valueOf(index, 18);
        this.bigDecimal128 = BigDecimal.valueOf(index, 38);
        this.bigDecimal256 = BigDecimal.valueOf(index, 76);

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

        this.dateTime_Local = ZonedDateTime.of(LocalDateTime.of(2026, 2, 11, 12, 45, 59), ZoneId.of("UTC")).toLocalDateTime();
        this.dateTime64_Local = ZonedDateTime.of(LocalDateTime.now(), ZoneId.of("UTC")).toLocalDateTime();

        this.dateTime_Zoned = ZonedDateTime.of(LocalDateTime.of(2026, 2, 11, 12, 45, 59), ZoneId.of("UTC"));
        this.dateTime64_Zoned = ZonedDateTime.of(LocalDateTime.now(), ZoneId.of("UTC"));

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

        this.tupleOfObjects =  new ArrayList<>();
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

    public BigInteger getBigInteger128() { return bigInteger128; }

    public BigInteger getBigInteger256() { return bigInteger256; }

    public int getUint8Primitive_Int() { return uint8Primitive_Int; }

    public Integer getUint8Object_Int() { return uint8Object_Int; }

    public short getUint8Primitive_Short() { return uint8Primitive_Short; }

    public Short getUint8Object_Short() { return uint8Object_Short; }

    public int getUint16Primitive() { return uint16Primitive; }

    public Integer getUint16Object() { return uint16Object; }

    public long getUint32Primitive() { return uint32Primitive; }

    public Long getUint32Object() { return uint32Object; }

    public long getUint64Primitive_Long() { return uint64Primitive_Long; }

    public Long getUint64Object_Long() { return uint64Object_Long; }

    public BigInteger getUint64Object_BigInt() { return uint64Object_BigInt; }

    public BigInteger getUint128Object() { return uint128Object; }

    public BigInteger getUint256Object() { return uint256Object; }

    public BigDecimal getBigDecimal() { return bigDecimal; }

    public BigDecimal getBigDecimal32() { return bigDecimal32; }

    public BigDecimal getBigDecimal64() { return bigDecimal64; }

    public BigDecimal getBigDecimal128() { return bigDecimal128; }

    public BigDecimal getBigDecimal256() { return bigDecimal256; }

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

    public boolean getBooleanPrimitive() { return booleanPrimitive; }

    public Boolean getBooleanObject() { return booleanObject; }

    public String getStr() { return str; }

    public String getFixedStr() { return fixedStr; }

    public LocalDate getDateObject() { return date; }

    public LocalDate getDate32Object() { return date32; }

    public LocalDateTime getDateTimeObject_Local() { return dateTime_Local; }

    public LocalDateTime getDateTime64Object_Local() { return dateTime64_Local; }

    public ZonedDateTime getDateTimeObject_Zoned() { return dateTime_Zoned; }

    public ZonedDateTime getDateTime64Object_Zoned() { return dateTime64_Zoned; }

    public UUID getUuid() { return uuid; }

    public List<String> getStringList() { return stringList; }

    public List<Long> getLongList() { return longList; }

    public Map<String, String> getMapOfStrings() { return mapOfStrings; }

    public List<Object> getTupleOfObjects() { return tupleOfObjects; }

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

    public void setUint8Primitive_Int(int uint8Primitive) {
        this.uint8Primitive_Int = uint8Primitive;
    }

    public void setUint8Object_Int(Object uint8Object) {
        if (uint8Object instanceof Short) {
            this.uint8Object_Int = ((Short)uint8Object).intValue();
        } else {
            this.uint8Object_Int = (Integer)uint8Object;
        }
    }

    public void setUint8Primitive_Short(short uint8Primitive) {
        this.uint8Primitive_Short = uint8Primitive;
    }

    public void setUint8Object_Short(Short uint8Object) {
        this.uint8Object_Short = uint8Object;
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

    public void setUint64Primitive_Long(Object uint64Primitive) {
        if (uint64Primitive instanceof BigInteger) {
            this.uint64Primitive_Long = ((BigInteger)uint64Primitive).longValue();
        } else {
            this.uint64Primitive_Long = (long)uint64Primitive;
        }
    }

    public void setUint64Object_Long(Object uint64Object) {
        if (uint64Object instanceof BigInteger) {
            this.uint64Object_Long = ((BigInteger)uint64Object).longValue();
        } else {
            this.uint64Object_Long = (Long)uint64Object;
        }
    }

    public void setUint64Object_BigInt(BigInteger uint64Object_BigInt) {
        this.uint64Object_BigInt = uint64Object_BigInt;
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

    public void setDateTimeObject_Local(Object dateTimeObject) {
        if (dateTimeObject instanceof ZonedDateTime) {
            ZoneId utcZone = ZoneId.of("UTC");
            this.dateTime_Local = ((ZonedDateTime)dateTimeObject).withZoneSameInstant(utcZone).toLocalDateTime();
        } else {
            this.dateTime_Local = (LocalDateTime)dateTimeObject;
        }
    }

    public void setDateTime64Object_Local(Object dateTime64Object) {
        if (dateTime64Object instanceof ZonedDateTime) {
            ZoneId utcZone = ZoneId.of("UTC");
            this.dateTime64_Local = ((ZonedDateTime)dateTime64Object).withZoneSameInstant(utcZone).toLocalDateTime();
        } else {
            this.dateTime64_Local = (LocalDateTime)dateTime64Object;
        }
    }

    public void setDateTimeObject_Zoned(ZonedDateTime dateTime_Zoned) {
        this.dateTime_Zoned = dateTime_Zoned;
    }

    public void setDateTime64Object_Zoned(ZonedDateTime dateTime64_Zoned) {
        this.dateTime64_Zoned = dateTime64_Zoned;
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

    public static String createTableSQL(String database, String tableName, int parts_to_throw_insert) {
        String createTable = createTableSQL(database, tableName);
        return createTable.trim().substring(0, createTable.trim().length() - 1) + " " + String.format("SETTINGS parts_to_throw_insert = %d;",  parts_to_throw_insert);
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
                "uint8Primitive_Int  UInt8," +
                "uint8Object_Int UInt8," +
                "uint8Primitive_Short  UInt8," +
                "uint8Object_Short UInt8," +
                "uint16Primitive  UInt16," +
                "uint16Object UInt16," +
                "uint32Primitive  UInt32," +
                "uint32Object UInt32," +
                "uint64Primitive_Long UInt64," +
                "uint64Object_Long UInt64," +
                "uint64Object_BigInt UInt64," +
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
                "dateTimeObject_Local DateTime," +
                "dateTime64Object_Local DateTime64(6, 'UTC')," +
                "dateTimeObject_Zoned DateTime," +
                "dateTime64Object_Zoned DateTime64(6, 'UTC')," +
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

        if (bytePrimitive != other.bytePrimitive) {
            System.out.println("bytePrimitive: this=" + bytePrimitive + ", other=" + other.bytePrimitive);
            return false;
        }
        if (!Objects.equals(byteObject, other.byteObject)) {
            System.out.println("byteObject: this=" + byteObject + ", other=" + other.byteObject);
            return false;
        }
        if (shortPrimitive != other.shortPrimitive) {
            System.out.println("shortPrimitive: this=" + shortPrimitive + ", other=" + other.shortPrimitive);
            return false;
        }
        if (!Objects.equals(shortObject, other.shortObject)) {
            System.out.println("shortObject: this=" + shortObject + ", other=" + other.shortObject);
            return false;
        }
        if (intPrimitive != other.intPrimitive) {
            System.out.println("intPrimitive: this=" + intPrimitive + ", other=" + other.intPrimitive);
            return false;
        }
        if (!Objects.equals(integerObject, other.integerObject)) {
            System.out.println("integerObject: this=" + integerObject + ", other=" + other.integerObject);
            return false;
        }
        if (longPrimitive != other.longPrimitive) {
            System.out.println("longPrimitive: this=" + longPrimitive + ", other=" + other.longPrimitive);
            return false;
        }
        if (!Objects.equals(longObject, other.longObject)) {
            System.out.println("longObject: this=" + longObject + ", other=" + other.longObject);
            return false;
        }

        if (uint8Primitive_Int != other.uint8Primitive_Int) {
            System.out.println("uint8Primitive_Int: this=" + uint8Primitive_Int + ", other=" + other.uint8Primitive_Int);
            return false;
        }
        if (!Objects.equals(uint8Object_Int, other.uint8Object_Int)) {
            System.out.println("uint8Object_Int: this=" + uint8Object_Int + ", other=" + other.uint8Object_Int);
            return false;
        }

        if (uint8Primitive_Short != other.uint8Primitive_Short) {
            System.out.println("uint8Primitive_Short: this=" + uint8Primitive_Short + ", other=" + other.uint8Primitive_Short);
            return false;
        }
        if (!Objects.equals(uint8Object_Short, other.uint8Object_Short)) {
            System.out.println("uint8Object_Short: this=" + uint8Object_Short + ", other=" + other.uint8Object_Short);
            return false;
        }
        if (uint16Primitive != other.uint16Primitive) {
            System.out.println("uint16Primitive: this=" + uint16Primitive + ", other=" + other.uint16Primitive);
            return false;
        }
        if (!Objects.equals(uint16Object, other.uint16Object)) {
            System.out.println("uint16Object: this=" + uint16Object + ", other=" + other.uint16Object);
            return false;
        }
        if (uint32Primitive != other.uint32Primitive) {
            System.out.println("uint32Primitive: this=" + uint32Primitive + ", other=" + other.uint32Primitive);
            return false;
        }
        if (!Objects.equals(uint32Object, other.uint32Object)) {
            System.out.println("uint32Object: this=" + uint32Object + ", other=" + other.uint32Object);
            return false;
        }
        if (!Objects.equals(uint64Primitive_Long, other.uint64Primitive_Long)) {
            System.out.println("uint64Primitive_Long: this=" + uint64Primitive_Long + ", other=" + other.uint64Primitive_Long);
            return false;
        }
        if (!Objects.equals(uint64Object_Long, other.uint64Object_Long)) {
            System.out.println("uint64Object_Long: this=" + uint64Object_Long + ", other=" + other.uint64Object_Long);
            return false;
        }
        if (!Objects.equals(uint64Object_BigInt, other.uint64Object_BigInt)) {
            System.out.println("uint64Object_BigInt: this=" + uint64Object_BigInt + ", other=" + other.uint64Object_BigInt);
            return false;
        }
        if (!Objects.equals(bigInteger128, other.bigInteger128)) {
            System.out.println("bigInteger128: this=" + bigInteger128 + ", other=" + other.bigInteger128);
            return false;
        }
        if (!Objects.equals(bigInteger256, other.bigInteger256)) {
            System.out.println("bigInteger256: this=" + bigInteger256 + ", other=" + other.bigInteger256);
            return false;
        }
        if (!Objects.equals(uint128Object, other.uint128Object)) {
            System.out.println("uint128Object: this=" + uint128Object + ", other=" + other.uint128Object);
            return false;
        }
        if (!Objects.equals(uint256Object, other.uint256Object)) {
            System.out.println("uint256Object: this=" + uint256Object + ", other=" + other.uint256Object);
            return false;
        }
        if (!Objects.equals(bigDecimal, other.bigDecimal)) {
            System.out.println("bigDecimal: this=" + bigDecimal + ", other=" + other.bigDecimal);
            return false;
        }
        if (!Objects.equals(bigDecimal32, other.bigDecimal32)) {
            System.out.println("bigDecimal32: this=" + bigDecimal32 + ", other=" + other.bigDecimal32);
            return false;
        }
        if (!Objects.equals(bigDecimal64, other.bigDecimal64)) {
            System.out.println("bigDecimal64: this=" + bigDecimal64 + ", other=" + other.bigDecimal64);
            return false;
        }
        if (!Objects.equals(bigDecimal128, other.bigDecimal128)) {
            System.out.println("bigDecimal128: this=" + bigDecimal128 + ", other=" + other.bigDecimal128);
            return false;
        }
        if (!Objects.equals(bigDecimal256, other.bigDecimal256)) {
            System.out.println("bigDecimal256: this=" + bigDecimal256 + ", other=" + other.bigDecimal256);
            return false;
        }
        if (Float.compare(floatPrimitive, other.floatPrimitive) != 0) {
            System.out.println("floatPrimitive: this=" + floatPrimitive + ", other=" + other.floatPrimitive);
            return false;
        }
        if (!Objects.equals(floatObject, other.floatObject)) {
            System.out.println("floatObject: this=" + floatObject + ", other=" + other.floatObject);
            return false;
        }
        if (Double.compare(doublePrimitive, other.doublePrimitive) != 0) {
            System.out.println("doublePrimitive: this=" + doublePrimitive + ", other=" + other.doublePrimitive);
            return false;
        }
        if (!Objects.equals(doubleObject, other.doubleObject)) {
            System.out.println("doubleObject: this=" + doubleObject + ", other=" + other.doubleObject);
            return false;
        }
        if (booleanPrimitive != other.booleanPrimitive) {
            System.out.println("booleanPrimitive: this=" + booleanPrimitive + ", other=" + other.booleanPrimitive);
            return false;
        }
        if (!Objects.equals(booleanObject, other.booleanObject)) {
            System.out.println("booleanObject: this=" + booleanObject + ", other=" + other.booleanObject);
            return false;
        }
        if (!Objects.equals(str, other.str)) {
            System.out.println("str: this=" + str + ", other=" + other.str);
            return false;
        }
        if (!Objects.equals(fixedStr, other.fixedStr)) {
            System.out.println("fixedStr: this=" + fixedStr + ", other=" + other.fixedStr);
            return false;
        }
        if (!Objects.equals(date, other.date)) {
            System.out.println("date: this=" + date + ", other=" + other.date);
            return false;
        }
        if (!Objects.equals(date32, other.date32)) {
            System.out.println("date32: this=" + date32 + ", other=" + other.date32);
            return false;
        }
        if (!Objects.equals(dateTime_Local, other.dateTime_Local)) {
            System.out.println("dateTime_Local: this=" + dateTime_Local + ", other=" + other.dateTime_Local);
            return false;
        }
        if (!Objects.equals(dateTime64_Local, other.dateTime64_Local)) {
            System.out.println("dateTime64_Local: this=" + dateTime64_Local + ", other=" + other.dateTime64_Local);
            return false;
        }
        if (!Objects.equals(dateTime_Zoned, other.dateTime_Zoned)) {
            System.out.println("dateTime_Zoned: this=" + dateTime_Zoned + ", other=" + other.dateTime_Zoned);
            return false;
        }
        if (!Objects.equals(dateTime64_Zoned, other.dateTime64_Zoned)) {
            System.out.println("dateTime64_Zoned: this=" + dateTime64_Zoned + ", other=" + other.dateTime64_Zoned);
            return false;
        }
        if (!Objects.equals(uuid, other.uuid)) {
            System.out.println("uuid: this=" + uuid + ", other=" + other.uuid);
            return false;
        }
        if (!Objects.equals(stringList, other.stringList)) {
            System.out.println("stringList: this=" + stringList + ", other=" + other.stringList);
            return false;
        }
        if (!Objects.equals(longList, other.longList)) {
            System.out.println("longList: this=" + longList + ", other=" + other.longList);
            return false;
        }
        if (!Objects.equals(mapOfStrings, other.mapOfStrings)) {
            System.out.println("mapOfStrings: this=" + mapOfStrings + ", other=" + other.mapOfStrings);
            return false;
        }
        if (!Objects.equals(tupleOfObjects, other.tupleOfObjects)) {
            System.out.println("tupleOfObjects: this=" + tupleOfObjects + ", other=" + other.tupleOfObjects);
            return false;
        }
        return true;
    }
}
