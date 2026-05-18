package org.apache.flink.connector.clickhouse.sink.convertor;

import com.clickhouse.data.ClickHouseDataType;
import org.apache.flink.connector.clickhouse.convertor.ColumnBinding;
import org.apache.flink.connector.clickhouse.convertor.DataMapper;
import org.apache.flink.connector.clickhouse.sink.pojo.CovidPOJO;

import java.util.List;
import java.util.Map;

public class CovidPOJODataMapper extends DataMapper<CovidPOJO> {

    @Override
    public void toMap(CovidPOJO input, Map<String, Object> map) {
        map.put("date", input.getLocalDate());
        map.put("location_key", input.getLocation_key());
        map.put("new_confirmed", input.getNew_confirmed());
        map.put("new_deceased", input.getNew_deceased());
        map.put("new_recovered", input.getNew_recovered());
        map.put("new_tested", input.getNew_tested());
        map.put("cumulative_confirmed", input.getCumulative_confirmed());
        map.put("cumulative_deceased", input.getCumulative_deceased());
        map.put("cumulative_recovered", input.getCumulative_recovered());
        map.put("cumulative_tested", input.getCumulative_tested());
    }

    @Override
    public List<ColumnBinding> bindings() {
        return List.of(
            ColumnBinding.scalar("date", "date", ClickHouseDataType.Date),
            ColumnBinding.scalar("location_key", "location_key", ClickHouseDataType.String, false, true),
            ColumnBinding.scalar("new_confirmed", "new_confirmed", ClickHouseDataType.Int32),
            ColumnBinding.scalar("new_deceased", "new_deceased", ClickHouseDataType.Int32),
            ColumnBinding.scalar("new_recovered", "new_recovered", ClickHouseDataType.Int32),
            ColumnBinding.scalar("new_tested", "new_tested", ClickHouseDataType.Int32),
            ColumnBinding.scalar("cumulative_confirmed", "cumulative_confirmed", ClickHouseDataType.Int32),
            ColumnBinding.scalar("cumulative_deceased", "cumulative_deceased", ClickHouseDataType.Int32),
            ColumnBinding.scalar("cumulative_recovered", "cumulative_recovered", ClickHouseDataType.Int32),
            ColumnBinding.scalar("cumulative_tested", "cumulative_tested", ClickHouseDataType.Int32)
        );
    }
}
