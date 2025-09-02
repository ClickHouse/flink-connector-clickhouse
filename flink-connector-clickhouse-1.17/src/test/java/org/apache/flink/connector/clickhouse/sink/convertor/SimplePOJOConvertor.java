package org.apache.flink.connector.clickhouse.sink.convertor;

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

        Serialize.writeFloat32(out, input.getFloatPrimitive(), false, false, ClickHouseDataType.Float32, false, "floatPrimitive");
        Serialize.writeFloat32(out, input.getFloatObject(), false, false, ClickHouseDataType.Float32, false, "floatObject");

        Serialize.writeFloat64(out, input.getDoublePrimitive(), false, false, ClickHouseDataType.Float64, false, "doublePrimitive");
        Serialize.writeFloat64(out, input.getDoubleObject(), false, false, ClickHouseDataType.Float64, false, "doubleObject");

        Serialize.writeBoolean(out, input.isBooleanPrimitive(), false, false, ClickHouseDataType.Bool, false, "booleanPrimitive");
        Serialize.writeBoolean(out, input.getBooleanObject(), false, false, ClickHouseDataType.Bool, false, "booleanObject");

        Serialize.writeString(out, input.getStr(), false, false, ClickHouseDataType.String, false, "str");
        Serialize.writeFixedString(out, input.getFixedStr(), false, false, ClickHouseDataType.FixedString, false, 10, "fixedStr");

    }
}
