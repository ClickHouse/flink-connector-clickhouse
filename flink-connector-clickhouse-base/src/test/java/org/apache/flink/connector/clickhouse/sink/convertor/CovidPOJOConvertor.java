package org.apache.flink.connector.clickhouse.sink.convertor;

import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.utils.Serialize;
import org.apache.flink.connector.clickhouse.convertor.POJOConvertor;
import org.apache.flink.connector.clickhouse.sink.pojo.CovidPOJO;

import java.io.IOException;
import java.io.OutputStream;

public class CovidPOJOConvertor extends POJOConvertor<CovidPOJO> {
    @Override
    public void instrument(OutputStream out, CovidPOJO input) throws IOException {
        Serialize.writeDate(out, input.getLocalDate(), false, false, ClickHouseDataType.Date, false, "date");
        Serialize.writeString(out, input.getLocation_key(), false, false, ClickHouseDataType.String, false, "location_key");
        Serialize.writeInt32(out, input.getNew_confirmed(), false, false, ClickHouseDataType.Int32, false, "new_confirmed");
        Serialize.writeInt32(out, input.getNew_deceased(), false, false, ClickHouseDataType.Int32, false, "");
        Serialize.writeInt32(out, input.getNew_recovered(), false, false, ClickHouseDataType.Int32, false, "");
        Serialize.writeInt32(out, input.getNew_tested(), false, false, ClickHouseDataType.Int32, false, "");
        Serialize.writeInt32(out, input.getCumulative_confirmed(), false, false, ClickHouseDataType.Int32, false, "");
        Serialize.writeInt32(out, input.getCumulative_deceased(), false, false, ClickHouseDataType.Int32, false, "");
        Serialize.writeInt32(out, input.getCumulative_recovered(), false, false, ClickHouseDataType.Int32, false, "");
        Serialize.writeInt32(out, input.getCumulative_tested(), false, false, ClickHouseDataType.Int32, false, "");
    }
}
