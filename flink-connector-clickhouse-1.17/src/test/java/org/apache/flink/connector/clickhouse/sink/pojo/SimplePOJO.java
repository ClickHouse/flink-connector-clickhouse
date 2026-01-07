package org.apache.flink.connector.clickhouse.sink.pojo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

public class SimplePOJO {

    private byte bytePrimitive;
    private Byte byteObject;

    private short shortPrimitive;
    private Short shortObject;

    private int intPrimitive;
    private Integer integerObject;

    private long longPrimitive;
    private Long longObject;

    private int uint8Primitive;
    private Integer uint8Object;

    private int uint16Primitive;
    private Integer uint16Object;

    private long uint32Primitive;
    private Long uint32Object;

    private long uint64Primitive;
    private Long uint64Object;

    private BigInteger uint128Object;
    private BigInteger uint256Object;

    private BigDecimal bigDecimal;
    private BigDecimal bigDecimal32;
    private BigDecimal bigDecimal64;
    private BigDecimal bigDecimal128;
    private BigDecimal bigDecimal256;

    private float floatPrimitive;
    private Float floatObject;

    private double doublePrimitive;
    private Double doubleObject;

    private boolean booleanPrimitive;
    private Boolean booleanObject;

    private String str;

    private String fixedStr;

    private BigInteger bigInteger128;
    private BigInteger bigInteger256;

    private LocalDate date;
    private LocalDate date32;
    private LocalDateTime dateTime;
    private LocalDateTime dateTime64;

    private UUID uuid;

    private List<String> stringList;
    private List<Long> longList;

    private Map<String, String> mapOfStrings;

    private List<Object> tupleOfObjects;

    private String jsonString;

    public SimplePOJO(int index) {
        this.bytePrimitive = Byte.MIN_VALUE;
        this.byteObject = Byte.MAX_VALUE;

        this.shortPrimitive = Short.MIN_VALUE;
        this.shortObject = Short.MAX_VALUE;

        this.intPrimitive = Integer.MIN_VALUE;
        this.integerObject = Integer.MAX_VALUE;

        this.longPrimitive = index;
        this.longObject = Long.MAX_VALUE;

        this.uint8Primitive = Byte.MAX_VALUE;
        this.uint8Object = (int)Byte.MAX_VALUE;

        this.uint16Primitive = Short.MAX_VALUE;
        this.uint16Object = (int)Short.MAX_VALUE;

        this.uint32Primitive = Integer.MAX_VALUE;
        this.uint32Object = (long)Integer.MAX_VALUE;

        this.uint64Primitive = Long.MAX_VALUE;
        this.uint64Object = Long.MAX_VALUE;

        this.uint128Object = BigInteger.valueOf(index);
        this.uint256Object = BigInteger.valueOf(index);

        this.bigDecimal = new BigDecimal(index);
        this.bigDecimal32 = new BigDecimal(index);
        this.bigDecimal64 = new BigDecimal(index);
        this.bigDecimal128 = new BigDecimal(index);
        this.bigDecimal256 = new BigDecimal(index);

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
        this.dateTime = LocalDateTime.now();
        this.dateTime64 = LocalDateTime.now();

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

        this.jsonString = String.format("{\"index\" : \"%d\", \"bar\" : \"foo\" }", index);
    }

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

    public int getUint8Primitive() { return uint8Primitive; }

    public Integer getUint8Object() { return uint8Object; }

    public int getUint16Primitive() { return uint16Primitive; }

    public Integer getUint16Object() { return uint16Object; }

    public long getUint32Primitive() { return uint32Primitive; }

    public Long getUint32Object() { return uint32Object; }

    public long getUint64Primitive() { return uint64Primitive; }

    public Long getUint64Object() { return uint64Object; }

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

    public boolean isBooleanPrimitive() { return booleanPrimitive; }

    public Boolean getBooleanObject() { return booleanObject; }

    public String getStr() { return str; }

    public BigInteger getBigInteger128() { return bigInteger128; }

    public BigInteger getBigInteger256() { return bigInteger256; }

    public String getFixedStr() { return fixedStr; }

    public LocalDate getDate() { return date; }

    public LocalDate getDate32() { return date32; }

    public LocalDateTime getDateTime() { return dateTime; }

    public LocalDateTime getDateTime64() { return dateTime64; }

    public UUID getUuid() { return uuid; }

    public List<String> getStringList() { return stringList; }

    public List<Long> getLongList() { return longList; }

    public Map<String, String> getMapOfStrings() { return mapOfStrings; }

    public List<Object> getTupleOfObjects() { return tupleOfObjects; }

    public String getJsonString() { return jsonString; }
}
