package com.example;

import com.clickhouse.data.ClickHouseDataType;
import org.apache.flink.connector.clickhouse.convertor.ColumnBinding;
import org.apache.flink.connector.clickhouse.convertor.DataMapper;

import java.util.List;
import java.util.Map;

/**
 * Build B: adds {@code c}. Matches the evolved table schema
 * {@code (a Int32, b String, c Nullable(String) DEFAULT 'omg')}.
 *
 * <p>When restoring from a Build-A checkpoint, restored Map entries lack the key
 * {@code "c"} entirely. {@code DataWriter.writeValue} dispatches against this
 * mapper's bindings, encountering a null value for {@code c}, and writes a NULL
 * to the wire. ClickHouse stores NULL (the table-level DEFAULT only fires when
 * the column is omitted from the wire-format header, not when an explicit null
 * is sent on a Nullable column).
 */
public class MapEvolutionMapperB extends DataMapper<Event> {
    @Override
    public void toMap(Event input, Map<String, Object> map) {
        map.put("a", input.getA());
        map.put("b", input.getB());
        map.put("c", input.getC()); // new field added in Build B (may be null)
    }

    @Override
    public List<ColumnBinding> bindings() {
        return List.of(
            ColumnBinding.scalar("a", "a", ClickHouseDataType.Int32),
            ColumnBinding.scalar("b", "b", ClickHouseDataType.String),
            // Nullable so we can encode null for restored Build-A entries (which
            // have no "c" key — Map.get(...) returns null).
            ColumnBinding.scalar("c", "c", ClickHouseDataType.String, true, false)
        );
    }
}
