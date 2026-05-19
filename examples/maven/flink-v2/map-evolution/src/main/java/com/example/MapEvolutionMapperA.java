package com.example;

import com.clickhouse.data.ClickHouseDataType;
import org.apache.flink.connector.clickhouse.convertor.ColumnBinding;
import org.apache.flink.connector.clickhouse.convertor.DataMapper;

import java.util.List;
import java.util.Map;

/**
 * Build A: only two fields ({@code a}, {@code b}). Matches the initial table
 * schema {@code (a Int32, b String)}.
 */
public class MapEvolutionMapperA extends DataMapper<Event> {
    @Override
    public void toMap(Event input, Map<String, Object> map) {
        map.put("a", input.getA());
        map.put("b", input.getB());
    }

    @Override
    public List<ColumnBinding> bindings() {
        return List.of(
            ColumnBinding.scalar("a", "a", ClickHouseDataType.Int32),
            ColumnBinding.scalar("b", "b", ClickHouseDataType.String)
        );
    }
}
