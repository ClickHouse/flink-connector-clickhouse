package org.apache.flink.connector.clickhouse.sink.convertor;

import com.clickhouse.data.ClickHouseDataType;
import org.apache.flink.connector.clickhouse.convertor.ColumnBinding;
import org.apache.flink.connector.clickhouse.convertor.DataMapper;
import org.apache.flink.connector.clickhouse.sink.pojo.SimplePOJOWithJSON;

import java.util.List;
import java.util.Map;

public class SimplePOJOWithJSONDataMapper extends DataMapper<SimplePOJOWithJSON> {

    @Override
    public void toMap(SimplePOJOWithJSON input, Map<String, Object> map) {
        map.put("longPrimitive", input.getLongPrimitive());
        map.put("jsonPayload", input.getJsonString());
    }

    @Override
    public List<ColumnBinding> bindings() {
        return List.of(
            ColumnBinding.scalar("longPrimitive", "longPrimitive", ClickHouseDataType.Int64),
            ColumnBinding.scalar("jsonPayload", "jsonPayload", ClickHouseDataType.JSON)
        );
    }
}
