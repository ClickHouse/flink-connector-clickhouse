package org.apache.flink.connector.clickhouse.sink.convertor;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.utils.writer.DataWriter;
import org.apache.flink.connector.clickhouse.convertor.POJOConvertor;
import org.apache.flink.connector.clickhouse.sink.pojo.SimplePOJO;

import java.io.IOException;

public class SimplePOJOConvertor extends POJOConvertor<SimplePOJO> {

    public SimplePOJOConvertor(boolean schemaHasDefaults) {
        super(schemaHasDefaults);
    }

    @Override
    public void instrument(DataWriter dataWriter, SimplePOJO input) throws IOException {
        dataWriter.writeInt8(input.getBytePrimitive(), false, ClickHouseDataType.Int8, false, "bytePrimitive");
        dataWriter.writeInt8(input.getByteObject(), false, ClickHouseDataType.Int8, false, "byteObject");

        dataWriter.writeInt16(input.getShortPrimitive(), false, ClickHouseDataType.Int16, false, "shortPrimitive");
        dataWriter.writeInt16(input.getShortObject(), false, ClickHouseDataType.Int16, false, "shortObject");

        dataWriter.writeInt32(input.getIntPrimitive(), false, ClickHouseDataType.Int32, false, "intPrimitive");
        dataWriter.writeInt32(input.getIntegerObject(), false, ClickHouseDataType.Int32, false, "integerObject");

        dataWriter.writeInt64(input.getLongPrimitive(), false, ClickHouseDataType.Int64, false, "longPrimitive");
        dataWriter.writeInt64(input.getLongObject(), false, ClickHouseDataType.Int64, false, "longObject");

        dataWriter.writeInt128(input.getBigInteger128(), false, ClickHouseDataType.Int128, false, "bigInteger128");
        dataWriter.writeInt256(input.getBigInteger256(), false, ClickHouseDataType.Int256, false, "bigInteger256");

        dataWriter.writeUInt8(input.getUint8Primitive_Int(), false, ClickHouseDataType.UInt8, false, "uint8Primitive_Int");
        dataWriter.writeUInt8(input.getUint8Object_Int(), false, ClickHouseDataType.UInt8, false, "uint8Object_Int");

        dataWriter.writeUInt8(input.getUint8Primitive_Short(), false, ClickHouseDataType.UInt8, false, "uint8Primitive_Short");
        dataWriter.writeUInt8(input.getUint8Object_Short(), false, ClickHouseDataType.UInt8, false, "uint8Object_Short");

        dataWriter.writeUInt16(input.getUint16Primitive(), false, ClickHouseDataType.UInt16, false, "uint16Primitive");
        dataWriter.writeUInt16(input.getUint16Object(), false, ClickHouseDataType.UInt16, false, "uint16Object");

        dataWriter.writeUInt32(input.getUint32Primitive(), false, ClickHouseDataType.UInt32, false, "uint32Primitive");
        dataWriter.writeUInt32(input.getUint32Object(), false, ClickHouseDataType.UInt32, false, "uint32Object");

        dataWriter.writeUInt64(input.getUint64Primitive_Long(), false, ClickHouseDataType.UInt64, false, "uint64Primitive_Long");
        dataWriter.writeUInt64(input.getUint64Object_Long(), false, ClickHouseDataType.UInt64, false, "uint64Object_Long");

        dataWriter.writeUInt64(input.getUint64Object_BigInt(), false, ClickHouseDataType.UInt64, false, "uint64Object_BigInt");

        dataWriter.writeUInt128(input.getUint128Object(), false, ClickHouseDataType.UInt128, false, "uint128Object");
        dataWriter.writeUInt256(input.getUint256Object(), false, ClickHouseDataType.UInt256, false, "uint256Object");

        dataWriter.writeDecimal(input.getBigDecimal(), false, ClickHouseDataType.Decimal, false, "bigDecimal", 10, 5);
        dataWriter.writeDecimal(input.getBigDecimal32(), false, ClickHouseDataType.Decimal32, false, "bigDecimal32", 9, 9);
        dataWriter.writeDecimal(input.getBigDecimal64(), false, ClickHouseDataType.Decimal64, false, "bigDecimal64", 18, 18);
        dataWriter.writeDecimal(input.getBigDecimal128(), false, ClickHouseDataType.Decimal128, false, "bigDecimal128", 38, 38);
        dataWriter.writeDecimal(input.getBigDecimal256(), false, ClickHouseDataType.Decimal256, false, "bigDecimal256", 76, 76);

        dataWriter.writeFloat32(input.getFloatPrimitive(), false, ClickHouseDataType.Float32, false, "floatPrimitive");
        dataWriter.writeFloat32(input.getFloatObject(), false, ClickHouseDataType.Float32, false, "floatObject");

        dataWriter.writeFloat64(input.getDoublePrimitive(), false, ClickHouseDataType.Float64, false, "doublePrimitive");
        dataWriter.writeFloat64(input.getDoubleObject(), false, ClickHouseDataType.Float64, false, "doubleObject");

        dataWriter.writeBoolean(input.getBooleanPrimitive(), false, ClickHouseDataType.Bool, false, "booleanPrimitive");
        dataWriter.writeBoolean(input.getBooleanObject(), false, ClickHouseDataType.Bool, false, "booleanObject");

        dataWriter.writeString(input.getStr(), false, ClickHouseDataType.String, false, "str");
        dataWriter.writeFixedString(input.getFixedStr(), false, ClickHouseDataType.FixedString, false, "fixedStr", 10);

        dataWriter.writeDate(input.getDateObject(), false, ClickHouseDataType.Date, false, "dateObject");
        dataWriter.writeDate32(input.getDate32Object(), false, ClickHouseDataType.Date32, false, "date32Object");

        dataWriter.writeDateTime(input.getDateTimeObject_Local(), false, ClickHouseDataType.DateTime, false, "dateTimeObject_Local");
        dataWriter.writeDateTime64(input.getDateTime64Object_Local(), false, ClickHouseDataType.DateTime64, false, "dateTime64Object_Local", 6);

        dataWriter.writeDateTime(input.getDateTimeObject_Zoned(), false, ClickHouseDataType.DateTime, false, "dateTimeObject_Zoned");
        dataWriter.writeDateTime64(input.getDateTime64Object_Zoned(), false, ClickHouseDataType.DateTime64, false, "dateTime64Object_Zoned", 6);

        dataWriter.writeUUID(input.getUuid(), false, ClickHouseDataType.UUID, false, "uuid");

        dataWriter.writeArray(input.getStringList(), ClickHouseColumn.of("stringList", ClickHouseDataType.Array, false, ClickHouseColumn.of("", ClickHouseDataType.String.toString())));

        dataWriter.writeArray(input.getLongList(), ClickHouseColumn.of("longList", ClickHouseDataType.Array, false, ClickHouseColumn.of("", ClickHouseDataType.Int64.toString())));

        dataWriter.writeMap(input.getMapOfStrings(), ClickHouseColumn.of("mapOfStrings", ClickHouseDataType.Map, false, ClickHouseColumn.of("", ClickHouseDataType.String.toString()), ClickHouseColumn.of("", ClickHouseDataType.String.toString())));

        dataWriter.writeTuple(input.getTupleOfObjects(), ClickHouseColumn.of("tupleOfObjects", ClickHouseDataType.Tuple, false, ClickHouseColumn.of("", ClickHouseDataType.String.toString()), ClickHouseColumn.of("", ClickHouseDataType.Int64.toString()), ClickHouseColumn.of("", ClickHouseDataType.Bool.toString())));

    }

}
