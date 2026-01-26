package org.apache.flink.connector.clickhouse.sink.convertor;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.utils.Serialize;
import com.clickhouse.utils.writer.DataWriter;
import org.apache.flink.connector.clickhouse.convertor.POJOConvertor;
import org.apache.flink.connector.clickhouse.sink.pojo.SimplePOJO;
import org.apache.flink.connector.clickhouse.sink.pojo.SimplePOJOWithJSON;

import java.io.IOException;
import java.io.OutputStream;

public class SimplePOJOWithJSONConvertor extends POJOConvertor<SimplePOJOWithJSON> {

    public SimplePOJOWithJSONConvertor(boolean hasDefaults) {
        super(hasDefaults);
    }

    @Override
    public void instrument(DataWriter dataWriter, SimplePOJOWithJSON input) throws IOException {
        dataWriter.writeInt64(input.getLongPrimitive(), false, ClickHouseDataType.Int64, false, "longPrimitive");
        dataWriter.writeJSON(input.getJsonString(), false, ClickHouseDataType.JSON, false, "jsonString");
    }

}
