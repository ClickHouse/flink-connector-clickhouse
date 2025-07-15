package org.apache.flink.connector.clickhouse.sink.pojo;

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
}
