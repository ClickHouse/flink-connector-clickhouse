package org.apache.flink.connector.clickhouse.sink.pojo;

import java.math.BigInteger;

public class SimplePOJO {

    private byte bytePrimitive;
    private Byte byteObject;

    private short shortPrimitive;
    private Short shortObject;

    private int intPrimitive;
    private Integer integerObject;

    private long longPrimitive;
    private Long longObject;

    private float floatPrimitive;
    private Float floatObject;

    private double doublePrimitive;
    private Double doubleObject;

    private boolean booleanPrimitive;
    private Boolean booleanObject;

    private String str;

    private BigInteger bigInteger128;
    private BigInteger bigInteger256;

    public SimplePOJO(int index) {
        this.bytePrimitive = Byte.MIN_VALUE;
        this.byteObject = Byte.MAX_VALUE;

        this.shortPrimitive = Short.MIN_VALUE;
        this.shortObject = Short.MAX_VALUE;

        this.intPrimitive = Integer.MIN_VALUE;
        this.integerObject = Integer.MAX_VALUE;

        this.longPrimitive = index;
        this.longObject = Long.MAX_VALUE;

        this.floatPrimitive = Float.MIN_VALUE;
        this.floatObject = Float.MAX_VALUE;

        this.doublePrimitive = Double.MIN_VALUE;
        this.doubleObject = Double.MAX_VALUE;

        this.booleanPrimitive = true;
        this.booleanObject = Boolean.FALSE;

        this.str = "str" + longPrimitive;

        this.bigInteger128 = BigInteger.valueOf(longPrimitive);
        this.bigInteger256 = BigInteger.valueOf(longPrimitive);
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
}
