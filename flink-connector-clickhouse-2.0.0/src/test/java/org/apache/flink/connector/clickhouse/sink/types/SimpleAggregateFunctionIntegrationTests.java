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
import java.util.ArrayList;
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

}
