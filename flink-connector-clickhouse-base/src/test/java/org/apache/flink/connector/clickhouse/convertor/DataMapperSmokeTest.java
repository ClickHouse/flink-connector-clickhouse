package org.apache.flink.connector.clickhouse.convertor;

import com.clickhouse.data.ClickHouseDataType;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DataMapperSmokeTest {

    static class Row { final int a; final String b; Row(int a, String b) { this.a = a; this.b = b; } }

    static class RowMapper extends DataMapper<Row> {
        @Override
        public void toMap(Row input, Map<String, Object> map) {
            map.put("a_key", input.a);
            map.put("b_key", input.b);
        }
        @Override
        public List<ColumnBinding> bindings() {
            return List.of(
                ColumnBinding.scalar("a_key", "col_a", ClickHouseDataType.Int32),
                ColumnBinding.scalar("b_key", "col_b", ClickHouseDataType.String)
            );
        }
    }

    @Test void toMapAndBindingsRoundTrip() {
        RowMapper m = new RowMapper();
        Map<String, Object> map = new LinkedHashMap<>();
        m.toMap(new Row(42, "hello"), map);
        assertEquals(42, map.get("a_key"));
        assertEquals("hello", map.get("b_key"));

        List<ColumnBinding> b = m.bindings();
        assertEquals(2, b.size());
        assertEquals("a_key", b.get(0).mapKey);
        assertEquals("col_a", b.get(0).columnName);
        assertEquals(ClickHouseDataType.Int32, b.get(0).column.getDataType());
    }
}
