package org.apache.flink.connector.clickhouse.sink.convertor;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.utils.writer.DataWriter;
import org.apache.flink.connector.clickhouse.convertor.POJOConvertor;
import org.apache.flink.connector.clickhouse.sink.pojo.SimplePOJO;

import java.io.IOException;
import java.io.OutputStream;

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

        // UIntX
        dataWriter.writeUInt8(input.getUint8Primitive(), false, ClickHouseDataType.UInt8, false, "uint8Primitive");
        dataWriter.writeUInt8(input.getUint8Object(), false, ClickHouseDataType.UInt8, false, "uint8Object");

        dataWriter.writeUInt16(input.getUint16Primitive(), false, ClickHouseDataType.UInt16, false, "uint8Primitive");
        dataWriter.writeUInt16(input.getUint16Object(), false, ClickHouseDataType.UInt16, false, "uint8Object");

        dataWriter.writeUInt32(input.getUint32Primitive(), false, ClickHouseDataType.UInt32, false, "uint8Primitive");
        dataWriter.writeUInt32(input.getUint32Object(), false, ClickHouseDataType.UInt32, false, "uint8Object");

        dataWriter.writeUInt64(input.getUint64Primitive(), false, ClickHouseDataType.UInt64, false, "uint8Primitive");
        dataWriter.writeUInt64(input.getUint64Object(), false, ClickHouseDataType.UInt64, false, "uint8Object");

        dataWriter.writeUInt128(input.getUint128Object(), false, ClickHouseDataType.UInt128, false, "bigInteger128");
        dataWriter.writeUInt256(input.getUint256Object(), false, ClickHouseDataType.UInt256, false, "bigInteger256");

        dataWriter.writeDecimal(input.getBigDecimal(), false, ClickHouseDataType.Decimal, false, "decimal", 10, 5);
        dataWriter.writeDecimal(input.getBigDecimal(), false, ClickHouseDataType.Decimal32, false, "decimal32", 9, 1);
        dataWriter.writeDecimal(input.getBigDecimal(), false, ClickHouseDataType.Decimal64, false, "decimal64", 18, 10);
        dataWriter.writeDecimal(input.getBigDecimal(), false, ClickHouseDataType.Decimal128, false, "decimal128", 38, 19);
        dataWriter.writeDecimal(input.getBigDecimal(), false, ClickHouseDataType.Decimal256, false, "decimal256", 76, 39);

        dataWriter.writeFloat32(input.getFloatPrimitive(), false, ClickHouseDataType.Float32, false, "floatPrimitive");
        dataWriter.writeFloat32(input.getFloatObject(), false, ClickHouseDataType.Float32, false, "floatObject");

        dataWriter.writeFloat64(input.getDoublePrimitive(), false, ClickHouseDataType.Float64, false, "doublePrimitive");
        dataWriter.writeFloat64(input.getDoubleObject(), false, ClickHouseDataType.Float64, false, "doubleObject");

        dataWriter.writeBoolean(input.isBooleanPrimitive(), false, ClickHouseDataType.Bool, false, "booleanPrimitive");
        dataWriter.writeBoolean(input.getBooleanObject(), false, ClickHouseDataType.Bool, false, "booleanObject");

        dataWriter.writeString(input.getStr(), false, ClickHouseDataType.String, false, "str");
        dataWriter.writeFixedString(input.getFixedStr(), false, ClickHouseDataType.FixedString, false, "fixedStr", 10);

        dataWriter.writeDate(input.getDate(), false, ClickHouseDataType.Date, false, "v_date");
        dataWriter.writeDate32(input.getDate32(), false, ClickHouseDataType.Date32, false, "v_date32");
        dataWriter.writeTimeDate(input.getDateTime(), false, ClickHouseDataType.DateTime, false, "v_dateTime");
        dataWriter.writeTimeDate64(input.getDateTime64(), false, ClickHouseDataType.DateTime64, false, "v_dateTime64", 1);

        dataWriter.writeUUID(input.getUuid(), false, ClickHouseDataType.UUID, false, "uuid");

        dataWriter.writeArray(input.getStringList(), ClickHouseColumn.of("stringList", ClickHouseDataType.Array, false, ClickHouseColumn.of("", ClickHouseDataType.String.toString())));

        dataWriter.writeArray(input.getLongList(), ClickHouseColumn.of("longList", ClickHouseDataType.Array, false, ClickHouseColumn.of("", ClickHouseDataType.Int64.toString())));

        dataWriter.writeMap(input.getMapOfStrings(), ClickHouseColumn.of("mapOfStrings", ClickHouseDataType.Map, false, ClickHouseColumn.of("", ClickHouseDataType.String.toString()), ClickHouseColumn.of("", ClickHouseDataType.String.toString())));

        dataWriter.writeTuple(input.getTupleOfObjects(), ClickHouseColumn.of("tupleOfObjects", ClickHouseDataType.Tuple, false, ClickHouseColumn.of("", ClickHouseDataType.String.toString()), ClickHouseColumn.of("", ClickHouseDataType.Int64.toString()), ClickHouseColumn.of("", ClickHouseDataType.Bool.toString())));

    }

}
