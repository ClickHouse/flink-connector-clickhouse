package org.apache.flink.connector.clickhouse.sink.convertor;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import org.apache.flink.connector.clickhouse.convertor.ColumnBinding;
import org.apache.flink.connector.clickhouse.convertor.DataMapper;
import org.apache.flink.connector.clickhouse.sink.pojo.SimplePOJO;

import java.util.List;
import java.util.Map;

public class SimplePOJODataMapper extends DataMapper<SimplePOJO> {

    @Override
    public void toMap(SimplePOJO input, Map<String, Object> map) {
        map.put("bytePrimitive", input.getBytePrimitive());
        map.put("byteObject", input.getByteObject());
        map.put("shortPrimitive", input.getShortPrimitive());
        map.put("shortObject", input.getShortObject());
        map.put("intPrimitive", input.getIntPrimitive());
        map.put("integerObject", input.getIntegerObject());
        map.put("longPrimitive", input.getLongPrimitive());
        map.put("longObject", input.getLongObject());
        map.put("bigInteger128", input.getBigInteger128());
        map.put("bigInteger256", input.getBigInteger256());
        map.put("uint8PrimitiveInt", input.getUint8PrimitiveInt());
        map.put("uint8ObjectInt", input.getUint8ObjectInt());
        map.put("uint8PrimitiveShort", input.getUint8PrimitiveShort());
        map.put("uint8ObjectShort", input.getUint8ObjectShort());
        map.put("uint16Primitive", input.getUint16Primitive());
        map.put("uint16Object", input.getUint16Object());
        map.put("uint32Primitive", input.getUint32Primitive());
        map.put("uint32Object", input.getUint32Object());
        map.put("uint64PrimitiveLong", input.getUint64PrimitiveLong());
        map.put("uint64ObjectLong", input.getUint64ObjectLong());
        map.put("uint64ObjectBigInt", input.getUint64ObjectBigInt());
        map.put("uint128Object", input.getUint128Object());
        map.put("uint256Object", input.getUint256Object());
        map.put("bigDecimal", input.getBigDecimal());
        map.put("bigDecimal32", input.getBigDecimal32());
        map.put("bigDecimal64", input.getBigDecimal64());
        map.put("bigDecimal128", input.getBigDecimal128());
        map.put("bigDecimal256", input.getBigDecimal256());
        map.put("floatPrimitive", input.getFloatPrimitive());
        map.put("floatObject", input.getFloatObject());
        map.put("doublePrimitive", input.getDoublePrimitive());
        map.put("doubleObject", input.getDoubleObject());
        map.put("booleanPrimitive", input.getBooleanPrimitive());
        map.put("booleanObject", input.getBooleanObject());
        map.put("str", input.getStr());
        map.put("fixedStr", input.getFixedStr());
        map.put("dateObject", input.getDateObject());
        map.put("date32Object", input.getDate32Object());
        map.put("dateTimeObjectLocal", input.getDateTimeObjectLocal());
        map.put("dateTime64ObjectLocal", input.getDateTime64ObjectLocal());
        map.put("dateTimeObjectZoned", input.getDateTimeObjectZoned());
        map.put("dateTime64ObjectZoned", input.getDateTime64ObjectZoned());
        map.put("uuid", input.getUuid());
        map.put("stringList", input.getStringList());
        map.put("longList", input.getLongList());
        map.put("mapOfStrings", input.getMapOfStrings());
        map.put("tupleOfObjects", input.getTupleOfObjects());
    }

    @Override
    public List<ColumnBinding> bindings() {
        return List.of(
            ColumnBinding.scalar("bytePrimitive", "bytePrimitive", ClickHouseDataType.Int8),
            ColumnBinding.scalar("byteObject", "byteObject", ClickHouseDataType.Int8),
            ColumnBinding.scalar("shortPrimitive", "shortPrimitive", ClickHouseDataType.Int16),
            ColumnBinding.scalar("shortObject", "shortObject", ClickHouseDataType.Int16),
            ColumnBinding.scalar("intPrimitive", "intPrimitive", ClickHouseDataType.Int32),
            ColumnBinding.scalar("integerObject", "integerObject", ClickHouseDataType.Int32),
            ColumnBinding.scalar("longPrimitive", "longPrimitive", ClickHouseDataType.Int64),
            ColumnBinding.scalar("longObject", "longObject", ClickHouseDataType.Int64),
            ColumnBinding.scalar("bigInteger128", "bigInteger128", ClickHouseDataType.Int128),
            ColumnBinding.scalar("bigInteger256", "bigInteger256", ClickHouseDataType.Int256),
            ColumnBinding.scalar("uint8PrimitiveInt", "uint8PrimitiveInt", ClickHouseDataType.UInt8),
            ColumnBinding.scalar("uint8ObjectInt", "uint8ObjectInt", ClickHouseDataType.UInt8),
            ColumnBinding.scalar("uint8PrimitiveShort", "uint8PrimitiveShort", ClickHouseDataType.UInt8),
            ColumnBinding.scalar("uint8ObjectShort", "uint8ObjectShort", ClickHouseDataType.UInt8),
            ColumnBinding.scalar("uint16Primitive", "uint16Primitive", ClickHouseDataType.UInt16),
            ColumnBinding.scalar("uint16Object", "uint16Object", ClickHouseDataType.UInt16),
            ColumnBinding.scalar("uint32Primitive", "uint32Primitive", ClickHouseDataType.UInt32),
            ColumnBinding.scalar("uint32Object", "uint32Object", ClickHouseDataType.UInt32),
            ColumnBinding.scalar("uint64PrimitiveLong", "uint64PrimitiveLong", ClickHouseDataType.UInt64),
            ColumnBinding.scalar("uint64ObjectLong", "uint64ObjectLong", ClickHouseDataType.UInt64),
            ColumnBinding.scalar("uint64ObjectBigInt", "uint64ObjectBigInt", ClickHouseDataType.UInt64),
            ColumnBinding.scalar("uint128Object", "uint128Object", ClickHouseDataType.UInt128),
            ColumnBinding.scalar("uint256Object", "uint256Object", ClickHouseDataType.UInt256),
            ColumnBinding.decimal("bigDecimal", "bigDecimal", 10, 5),
            ColumnBinding.decimal("bigDecimal32", "bigDecimal32", 9, 9),
            ColumnBinding.decimal("bigDecimal64", "bigDecimal64", 18, 18),
            ColumnBinding.decimal("bigDecimal128", "bigDecimal128", 38, 38),
            ColumnBinding.decimal("bigDecimal256", "bigDecimal256", 76, 76),
            ColumnBinding.scalar("floatPrimitive", "floatPrimitive", ClickHouseDataType.Float32),
            ColumnBinding.scalar("floatObject", "floatObject", ClickHouseDataType.Float32),
            ColumnBinding.scalar("doublePrimitive", "doublePrimitive", ClickHouseDataType.Float64),
            ColumnBinding.scalar("doubleObject", "doubleObject", ClickHouseDataType.Float64),
            ColumnBinding.scalar("booleanPrimitive", "booleanPrimitive", ClickHouseDataType.Bool),
            ColumnBinding.scalar("booleanObject", "booleanObject", ClickHouseDataType.Bool),
            ColumnBinding.scalar("str", "str", ClickHouseDataType.String),
            ColumnBinding.fixedString("fixedStr", "fixedStr", 10),
            ColumnBinding.scalar("dateObject", "dateObject", ClickHouseDataType.Date),
            ColumnBinding.scalar("date32Object", "date32Object", ClickHouseDataType.Date32),
            ColumnBinding.scalar("dateTimeObjectLocal", "dateTimeObjectLocal", ClickHouseDataType.DateTime),
            ColumnBinding.dateTime64("dateTime64ObjectLocal", "dateTime64ObjectLocal", 6, "UTC"),
            ColumnBinding.scalar("dateTimeObjectZoned", "dateTimeObjectZoned", ClickHouseDataType.DateTime),
            ColumnBinding.dateTime64("dateTime64ObjectZoned", "dateTime64ObjectZoned", 6, "UTC"),
            ColumnBinding.scalar("uuid", "uuid", ClickHouseDataType.UUID),
            ColumnBinding.array("stringList", "stringList",
                ClickHouseColumn.of("", ClickHouseDataType.String.toString())),
            ColumnBinding.array("longList", "longList",
                ClickHouseColumn.of("", ClickHouseDataType.Int64.toString())),
            ColumnBinding.map("mapOfStrings", "mapOfStrings",
                ClickHouseColumn.of("", ClickHouseDataType.String.toString()),
                ClickHouseColumn.of("", ClickHouseDataType.String.toString())),
            ColumnBinding.tuple("tupleOfObjects", "tupleOfObjects",
                ClickHouseColumn.of("", ClickHouseDataType.String.toString()),
                ClickHouseColumn.of("", ClickHouseDataType.Int64.toString()),
                ClickHouseColumn.of("", ClickHouseDataType.Bool.toString()))
        );
    }
}
