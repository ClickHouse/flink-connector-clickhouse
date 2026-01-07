package org.apache.flink.connector.clickhouse.sink.convertor;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.utils.Serialize;
import org.apache.flink.connector.clickhouse.convertor.POJOConvertor;
import org.apache.flink.connector.clickhouse.sink.pojo.SimplePOJO;

import java.io.IOException;
import java.io.OutputStream;

public class SimplePOJOConvertor extends POJOConvertor<SimplePOJO> {
    @Override
    public void instrument(OutputStream out, SimplePOJO input) throws IOException {
        Serialize.writeInt8(out, input.getBytePrimitive(), false, false, ClickHouseDataType.Int8, false, "bytePrimitive");
        Serialize.writeInt8(out, input.getByteObject(), false, false, ClickHouseDataType.Int8, false, "byteObject");

        Serialize.writeInt16(out, input.getShortPrimitive(), false, false, ClickHouseDataType.Int16, false, "shortPrimitive");
        Serialize.writeInt16(out, input.getShortObject(), false, false, ClickHouseDataType.Int16, false, "shortObject");

        Serialize.writeInt32(out, input.getIntPrimitive(), false, false, ClickHouseDataType.Int32, false, "intPrimitive");
        Serialize.writeInt32(out, input.getIntegerObject(), false, false, ClickHouseDataType.Int32, false, "integerObject");

        Serialize.writeInt64(out, input.getLongPrimitive(), false, false, ClickHouseDataType.Int64, false, "longPrimitive");
        Serialize.writeInt64(out, input.getLongObject(), false, false, ClickHouseDataType.Int64, false, "longObject");

        Serialize.writeInt128(out, input.getBigInteger128(), false, false, ClickHouseDataType.Int128, false, "bigInteger128");
        Serialize.writeInt256(out, input.getBigInteger256(), false, false, ClickHouseDataType.Int256, false, "bigInteger256");

        // UIntX
        Serialize.writeUInt8(out, input.getUint8Primitive(), false, false, ClickHouseDataType.UInt8, false, "uint8Primitive");
        Serialize.writeUInt8(out, input.getUint8Object(), false, false, ClickHouseDataType.UInt8, false, "uint8Object");

        Serialize.writeUInt16(out, input.getUint16Primitive(), false, false, ClickHouseDataType.UInt16, false, "uint8Primitive");
        Serialize.writeUInt16(out, input.getUint16Object(), false, false, ClickHouseDataType.UInt16, false, "uint8Object");

        Serialize.writeUInt32(out, input.getUint32Primitive(), false, false, ClickHouseDataType.UInt32, false, "uint8Primitive");
        Serialize.writeUInt32(out, input.getUint32Object(), false, false, ClickHouseDataType.UInt32, false, "uint8Object");

        Serialize.writeUInt64(out, input.getUint64Primitive(), false, false, ClickHouseDataType.UInt64, false, "uint8Primitive");
        Serialize.writeUInt64(out, input.getUint64Object(), false, false, ClickHouseDataType.UInt64, false, "uint8Object");

        Serialize.writeUInt128(out, input.getUint128Object(), false, false, ClickHouseDataType.UInt128, false, "bigInteger128");
        Serialize.writeUInt256(out, input.getUint256Object(), false, false, ClickHouseDataType.UInt256, false, "bigInteger256");

        Serialize.writeDecimal(out, input.getBigDecimal(), false, false, ClickHouseDataType.Decimal, false, "decimal", 10, 5);
        Serialize.writeDecimal(out, input.getBigDecimal(), false, false, ClickHouseDataType.Decimal32, false, "decimal32", 9, 1);
        Serialize.writeDecimal(out, input.getBigDecimal(), false, false, ClickHouseDataType.Decimal64, false, "decimal64", 18, 10);
        Serialize.writeDecimal(out, input.getBigDecimal(), false, false, ClickHouseDataType.Decimal128, false, "decimal128", 38, 19);
        Serialize.writeDecimal(out, input.getBigDecimal(), false, false, ClickHouseDataType.Decimal256, false, "decimal256", 76, 39);

        Serialize.writeFloat32(out, input.getFloatPrimitive(), false, false, ClickHouseDataType.Float32, false, "floatPrimitive");
        Serialize.writeFloat32(out, input.getFloatObject(), false, false, ClickHouseDataType.Float32, false, "floatObject");

        Serialize.writeFloat64(out, input.getDoublePrimitive(), false, false, ClickHouseDataType.Float64, false, "doublePrimitive");
        Serialize.writeFloat64(out, input.getDoubleObject(), false, false, ClickHouseDataType.Float64, false, "doubleObject");

        Serialize.writeBoolean(out, input.isBooleanPrimitive(), false, false, ClickHouseDataType.Bool, false, "booleanPrimitive");
        Serialize.writeBoolean(out, input.getBooleanObject(), false, false, ClickHouseDataType.Bool, false, "booleanObject");

        Serialize.writeString(out, input.getStr(), false, false, ClickHouseDataType.String, false, "str");
        Serialize.writeFixedString(out, input.getFixedStr(), false, false, ClickHouseDataType.FixedString, false, "fixedStr", 10);

        Serialize.writeDate(out, input.getDate(), false, false, ClickHouseDataType.Date, false, "v_date");
        Serialize.writeDate32(out, input.getDate32(), false, false, ClickHouseDataType.Date32, false, "v_date32");
        Serialize.writeTimeDate(out, input.getDateTime(), false, false, ClickHouseDataType.DateTime, false, "v_dateTime");
        Serialize.writeTimeDate64(out, input.getDateTime64(), false, false, ClickHouseDataType.DateTime64, false, "v_dateTime64", 1);

        Serialize.writeUUID(out, input.getUuid(), false, false, ClickHouseDataType.UUID, false, "uuid");

        Serialize.writeArray(out, input.getStringList(), ClickHouseColumn.of("stringList", ClickHouseDataType.Array, false, ClickHouseColumn.of("", ClickHouseDataType.String.toString())));

        Serialize.writeArray(out, input.getLongList(), ClickHouseColumn.of("longList", ClickHouseDataType.Array, false, ClickHouseColumn.of("", ClickHouseDataType.Int64.toString())));

        Serialize.writeMap(out, input.getMapOfStrings(), ClickHouseColumn.of("mapOfStrings", ClickHouseDataType.Map, false, ClickHouseColumn.of("", ClickHouseDataType.String.toString()), ClickHouseColumn.of("", ClickHouseDataType.String.toString())));

        Serialize.writeTuple(out, input.getTupleOfObjects(), ClickHouseColumn.of("tupleOfObjects", ClickHouseDataType.Tuple, false, ClickHouseColumn.of("", ClickHouseDataType.String.toString()), ClickHouseColumn.of("", ClickHouseDataType.Int64.toString()), ClickHouseColumn.of("", ClickHouseDataType.Bool.toString()) ));

        Serialize.writeString(out, input.getJsonString(), false, false, ClickHouseDataType.JSON, false, "jsonString");
    }

}
