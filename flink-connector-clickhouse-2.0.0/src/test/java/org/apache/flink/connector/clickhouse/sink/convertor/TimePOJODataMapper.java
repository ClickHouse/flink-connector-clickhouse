package org.apache.flink.connector.clickhouse.sink.convertor;

import com.clickhouse.data.ClickHouseDataType;
import org.apache.flink.connector.clickhouse.convertor.ColumnBinding;
import org.apache.flink.connector.clickhouse.convertor.DataMapper;
import org.apache.flink.connector.clickhouse.sink.pojo.TimePOJO;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class TimePOJODataMapper extends DataMapper<TimePOJO> {

    private final TimePOJO.DataType columnType;
    private final int precision;

    public TimePOJODataMapper(TimePOJO.DataType columnType, int precision) {
        this.columnType = columnType;
        this.precision = precision;
    }

    @Override
    public void toMap(TimePOJO input, Map<String, Object> map) {
        map.put("id", input.id);
        map.put("elapsed", Duration.ofNanos(input.elapsedNanos));
    }

    @Override
    public List<ColumnBinding> bindings() {
        ColumnBinding elapsedBinding =
            columnType == TimePOJO.DataType.TIME64
                ? ColumnBinding.time64("elapsed", "elapsed", precision)
                : ColumnBinding.scalar("elapsed", "elapsed", ClickHouseDataType.Time);
        return List.of(
            ColumnBinding.scalar("id", "id", ClickHouseDataType.String),
            elapsedBinding
        );
    }
}
