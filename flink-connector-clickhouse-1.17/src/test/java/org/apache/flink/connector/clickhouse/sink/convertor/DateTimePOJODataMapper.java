package org.apache.flink.connector.clickhouse.sink.convertor;

import com.clickhouse.data.ClickHouseDataType;
import org.apache.flink.connector.clickhouse.convertor.ColumnBinding;
import org.apache.flink.connector.clickhouse.convertor.DataMapper;
import org.apache.flink.connector.clickhouse.sink.pojo.DateTimePOJO;

import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

public class DateTimePOJODataMapper extends DataMapper<DateTimePOJO> {

    private final DateTimePOJO.DataType columnType;
    private final int precision;

    public DateTimePOJODataMapper(DateTimePOJO.DataType columnType, int precision) {
        this.columnType = columnType;
        this.precision = precision;
    }

    @Override
    public void toMap(DateTimePOJO input, Map<String, Object> map) {
        map.put("id", input.id);
        map.put("created_at", input.createdAt.atZone(ZoneOffset.UTC));
        map.put("num_logins", input.numLogins);
    }

    @Override
    public List<ColumnBinding> bindings() {
        ColumnBinding createdAtBinding =
            columnType == DateTimePOJO.DataType.DATETIME64
                ? ColumnBinding.dateTime64("created_at", "created_at", precision)
                : ColumnBinding.scalar("created_at", "created_at", ClickHouseDataType.DateTime);
        return List.of(
            ColumnBinding.scalar("id", "id", ClickHouseDataType.String),
            createdAtBinding,
            ColumnBinding.scalar("num_logins", "num_logins", ClickHouseDataType.Int32)
        );
    }
}
