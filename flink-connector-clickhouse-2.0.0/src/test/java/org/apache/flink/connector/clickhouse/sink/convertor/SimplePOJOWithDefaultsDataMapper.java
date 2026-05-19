package org.apache.flink.connector.clickhouse.sink.convertor;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import org.apache.flink.connector.clickhouse.convertor.ColumnBinding;
import org.apache.flink.connector.clickhouse.convertor.DataMapper;
import org.apache.flink.connector.clickhouse.sink.pojo.SimplePOJOWithDefaults;

import java.util.List;
import java.util.Map;

/**
 * Even-indexed POJOs have {@code createdOn == null}; with
 * {@code input_format_null_as_default = 1} (set by the writer in T9), the server
 * substitutes the column's {@code DEFAULT} expression for those rows.
 */
public class SimplePOJOWithDefaultsDataMapper extends DataMapper<SimplePOJOWithDefaults> {

    @Override
    public void toMap(SimplePOJOWithDefaults input, Map<String, Object> map) {
        map.put("id", input.getId());
        map.put("created_on", input.getCreatedOn());
    }

    @Override
    public List<ColumnBinding> bindings() {
        return List.of(
            ColumnBinding.scalar("id", "id", ClickHouseDataType.Int32),
            ColumnBinding.of("created_on", "created_on",
                ClickHouseColumn.of("created_on", "Nullable(DateTime64(6, 'UTC'))"))
        );
    }
}
