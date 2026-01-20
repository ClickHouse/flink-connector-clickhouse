package org.apache.flink.connector.clickhouse.sink.convertor;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.utils.Serialize;
import org.apache.flink.connector.clickhouse.convertor.POJOConvertor;
import org.apache.flink.connector.clickhouse.sink.pojo.SimplePOJO;
import org.apache.flink.connector.clickhouse.sink.pojo.SimplePOJOWithJSON;

import java.io.IOException;
import java.io.OutputStream;

public class SimplePOJOWithJSONConvertor extends POJOConvertor<SimplePOJOWithJSON> {
    @Override
    public void instrument(OutputStream out, SimplePOJOWithJSON input) throws IOException {
        Serialize.writeInt64(out, input.getLongPrimitive(), false, false, ClickHouseDataType.Int64, false, "longPrimitive");

        Serialize.writeJSON(out, input.getJsonString(), false, false, ClickHouseDataType.JSON, false, "jsonString");
    }

}
