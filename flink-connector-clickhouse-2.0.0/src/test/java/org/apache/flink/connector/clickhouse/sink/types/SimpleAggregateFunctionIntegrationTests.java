package org.apache.flink.connector.clickhouse.sink.types;

import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.convertor.ColumnBinding;
import org.apache.flink.connector.clickhouse.convertor.DataMapper;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink;
import org.apache.flink.connector.test.FlinkClusterTests;
import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.clickhouse.sink.ClickHouseSinkTestUtils.*;

/**
 * End-to-end coverage for {@code SimpleAggregateFunction(f, T)} columns (issue #143).
 *
 * <p>{@code SimpleAggregateFunction} is wire-encoded exactly as its inner type T,
 * while the {@code RowBinaryWithNamesAndTypes} header names the wrapper type. Only a
 * real server enforces that the two agree, which is what these tests add on top of the
 * byte-level assertions in {@code DataWriterDispatchTest}: they ingest through the
 * production sink and read the values back.
 *
 * <p>The inner types are chosen to cover each property the wrapper has to forward from
 * its nested column — nullability, precision/scale, and the LowCardinality prefix —
 * since the outer column reports none of them.
 */
public class SimpleAggregateFunctionIntegrationTests extends FlinkClusterTests {


    public static final class AggRow implements Serializable {
        public final int id;
        public final long maxVal;
        public final String lastName;
        public final BigDecimal total;
        public final String tag;

        public AggRow(int id, long maxVal, String lastName, BigDecimal total, String tag) {
            this.id = id;
            this.maxVal = maxVal;
            this.lastName = lastName;
            this.total = total;
            this.tag = tag;
        }
    }

    public static final class AggRowMapper extends DataMapper<AggRow> {
        @Override
        public void toMap(AggRow r, Map<String, Object> map) {
            map.put("id", r.id);
            map.put("max_val", r.maxVal);
            map.put("last_name", r.lastName);
            map.put("total", r.total);
            map.put("tag", r.tag);
        }

        @Override
        public List<ColumnBinding> bindings() {
            return List.of(
                ColumnBinding.scalar("id", "id", ClickHouseDataType.Int32),
                saf("max_val", "max", "Int64"),
                saf("last_name", "anyLast", "Nullable(String)"),
                saf("total", "sum", "Decimal(38, 4)"),
                saf("tag", "anyLast", "LowCardinality(String)")
            );
        }

        /** How a user expresses a SimpleAggregateFunction column: a raw type expression. */
        private static ColumnBinding saf(String column, String function, String innerType) {
            return ColumnBinding.of(column, column, ClickHouseColumn.of(
                column, "SimpleAggregateFunction(" + function + ", " + innerType + ")"));
        }
    }

    @Test
    void simpleAggregateFunctionColumnsRoundTrip() throws Exception {
        String tableName = "simple_aggregate_function_test";

        // Decimal storage type is Decimal(38, 4), not Decimal(18, 4): ClickHouse requires
        // the column type to equal the aggregate's return type, and sum widens to 38 digits.
        ClickHouseServerForTests.executeSql(
            "CREATE TABLE `" + getDatabase() + "`.`" + tableName + "` ("
                + "  id Int32, "
                + "  max_val SimpleAggregateFunction(max, Int64), "
                + "  last_name SimpleAggregateFunction(anyLast, Nullable(String)), "
                + "  total SimpleAggregateFunction(sum, Decimal(38, 4)), "
                + "  tag SimpleAggregateFunction(anyLast, LowCardinality(String))"
                + ") ENGINE=AggregatingMergeTree ORDER BY id");

        ClickHouseAsyncSink<AggRow> sink = buildSink(
            new ClickHouseConvertor<>(AggRow.class, new AggRowMapper()),
            tableName);

        // Distinct ids: anyLast is nondeterministic across parts, so rows must not
        // collide on the sorting key for the read-back to be stable.
        List<AggRow> rows = new ArrayList<>();
        rows.add(new AggRow(1, 10L, "alpha", new BigDecimal("1.5000"), "x"));
        // Null on the Nullable inner type: the outer wrapper reports non-nullable,
        // so nullability has to be taken from the nested column.
        rows.add(new AggRow(2, 20L, null, new BigDecimal("2.2500"), "y"));

        runJob(sink, rows, tableName, 2);

        List<GenericRecord> records = ClickHouseServerForTests.extractAllData(
            getDatabase(), tableName, "id");
        Assertions.assertEquals(2, records.size());

        GenericRecord first = records.get(0);
        Assertions.assertEquals(1, first.getInteger("id"));
        Assertions.assertEquals(10L, first.getLong("max_val"));
        Assertions.assertEquals("alpha", first.getString("last_name"));
        Assertions.assertEquals(0, new BigDecimal("1.5000").compareTo(first.getBigDecimal("total")));
        Assertions.assertEquals("x", first.getString("tag"));

        GenericRecord second = records.get(1);
        Assertions.assertEquals(2, second.getInteger("id"));
        Assertions.assertEquals(20L, second.getLong("max_val"));
        Assertions.assertFalse(second.hasValue("last_name"),
            "Null on SimpleAggregateFunction(anyLast, Nullable(String)) should be stored as NULL");
        Assertions.assertEquals(0, new BigDecimal("2.2500").compareTo(second.getBigDecimal("total")));
        Assertions.assertEquals("y", second.getString("tag"));
    }

    // Inner types whose encoding is multi-part or length/scale-dependent, so only a
    // server can confirm the body still matches the wrapper named in the header.

    /** Flink-POJO conformant, so the composite fields avoid Kryo. */
    public static class CompositeRow implements Serializable {
        public int id;
        public ArrayList<String> arr;
        public HashMap<String, Long> kv;
        public ArrayList<Object> tup;
        public String code;
        public LocalDateTime ts;

        public CompositeRow() {}

        CompositeRow(int id) {
            this.id = id;
            this.arr = new ArrayList<>(List.of("a" + id, "b" + id));
            this.kv = new HashMap<>(Map.of("k" + id, (long) id));
            this.tup = new ArrayList<>(List.of(id, "t" + id));
            this.code = "cd0" + id;
            this.ts = LocalDateTime.of(2024, 3, 10, 8, 45, id, 123_000_000);
        }
    }

    public static final class CompositeRowMapper extends DataMapper<CompositeRow> {
        @Override
        public void toMap(CompositeRow r, Map<String, Object> map) {
            map.put("id", r.id);
            map.put("arr", r.arr);
            map.put("kv", r.kv);
            map.put("tup", r.tup);
            map.put("code", r.code);
            map.put("ts", r.ts);
        }

        @Override
        public List<ColumnBinding> bindings() {
            return List.of(
                ColumnBinding.scalar("id", "id", ClickHouseDataType.Int32),
                saf("arr", "groupArrayArray", "Array(String)"),
                saf("kv", "sumMap", "Map(String, UInt64)"),
                saf("tup", "anyLast", "Tuple(Int32, String)"),
                saf("code", "anyLast", "FixedString(4)"),
                saf("ts", "anyLast", "DateTime64(3, 'UTC')")
            );
        }

        private static ColumnBinding saf(String column, String function, String innerType) {
            return ColumnBinding.of(column, column, ClickHouseColumn.of(
                column, "SimpleAggregateFunction(" + function + ", " + innerType + ")"));
        }
    }

    @Test
    void compositeInnerTypesRoundTrip() throws Exception {
        String tableName = "simple_aggregate_function_composite_test";

        ClickHouseServerForTests.executeSql(
            "CREATE TABLE `" + getDatabase() + "`.`" + tableName + "` ("
                + "  id Int32, "
                + "  arr SimpleAggregateFunction(groupArrayArray, Array(String)), "
                + "  kv SimpleAggregateFunction(sumMap, Map(String, UInt64)), "
                + "  tup SimpleAggregateFunction(anyLast, Tuple(Int32, String)), "
                + "  code SimpleAggregateFunction(anyLast, FixedString(4)), "
                + "  ts SimpleAggregateFunction(anyLast, DateTime64(3, 'UTC'))"
                + ") ENGINE=AggregatingMergeTree ORDER BY id");

        ClickHouseAsyncSink<CompositeRow> sink = buildSink(
            new ClickHouseConvertor<>(CompositeRow.class, new CompositeRowMapper()),
            tableName);

        List<CompositeRow> rows = new ArrayList<>();
        rows.add(new CompositeRow(1));
        rows.add(new CompositeRow(2));

        runJob(sink, rows, tableName, 2);

        List<GenericRecord> records = ClickHouseServerForTests.extractAllData(
            getDatabase(), tableName, "id");
        Assertions.assertEquals(2, records.size());

        for (int i = 0; i < 2; i++) {
            int id = i + 1;
            GenericRecord rec = records.get(i);
            Assertions.assertEquals(id, rec.getInteger("id"));
            Assertions.assertEquals(List.of("a" + id, "b" + id), rec.getList("arr"));
            Assertions.assertArrayEquals(new Object[]{id, "t" + id}, rec.getTuple("tup"));
            Assertions.assertEquals("cd0" + id, rec.getString("code"));
            // Milliseconds prove the DateTime64 scale reached the writer.
            Assertions.assertEquals(LocalDateTime.of(2024, 3, 10, 8, 45, id, 123_000_000),
                rec.getLocalDateTime("ts"));
            // Map has no typed accessor on GenericRecord.
            Map<?, ?> kv = (Map<?, ?>) rec.getObject("kv");
            Assertions.assertEquals(1, kv.size());
            Assertions.assertEquals(id, ((Number) kv.get("k" + id)).intValue());
        }
    }

}
