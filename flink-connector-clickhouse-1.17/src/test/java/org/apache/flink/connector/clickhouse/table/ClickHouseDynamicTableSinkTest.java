package org.apache.flink.connector.clickhouse.table;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.config.RetryPolicy;
import com.clickhouse.data.ClickHouseColumn;

import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.connector.clickhouse.table.data.RowDataDataMapper;
import org.apache.flink.connector.clickhouse.table.schema.SchemaResolver;
import org.apache.flink.connector.clickhouse.table.schema.SchemaResolverOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class ClickHouseDynamicTableSinkTest {

    /** Built the way the factory builds it, minus the network: the no-ping config constructor. */
    private static ClickHouseDynamicTableSink sink() {
        ClickHouseClientConfig config = new ClickHouseClientConfig("http://localhost:1", "u", "", "db", "t",
                Map.of(), Map.of(), RetryPolicy.forever());
        RowDataDataMapper mapper = RowDataDataMapper.of(SchemaResolver.resolve(
                ResolvedSchema.of(Column.physical("id", DataTypes.BIGINT().notNull())),
                new TableSchema(ClickHouseColumn.parse("id Int64")),
                new SchemaResolverOptions("db", "t", ZoneId.of("UTC"), false)));
        return new ClickHouseDynamicTableSink(config, mapper, 500, 5L * 1024 * 1024, 5_000L, 50, 10_000,
                1L * 1024 * 1024, null, "db.t");
    }

    /** The planner copies the sink per INSERT of a statement set; the copies must not share the config. */
    @Test void copyIsADistinctSinkWithItsOwnClientConfig() throws Exception {
        ClickHouseDynamicTableSink original = sink();
        DynamicTableSink copy = original.copy();

        assertNotSame(original, copy);
        assertEquals(original.asSummaryString(), copy.asSummaryString());
        configOf(copy).setEnableJsonSupportAsString(true);
        assertFalse(configOf(original).getEnableJsonSupportAsString());
    }

    @Test void changelogModeIsInsertOnlyWhateverThePlannerRequests() {
        assertEquals(ChangelogMode.insertOnly(), sink().getChangelogMode(ChangelogMode.all()));
        assertEquals(ChangelogMode.insertOnly(), sink().getChangelogMode(ChangelogMode.upsert()));
    }

    private static ClickHouseClientConfig configOf(DynamicTableSink sink) throws Exception {
        Field field = ClickHouseDynamicTableSink.class.getDeclaredField("clientConfig");
        field.setAccessible(true);
        return (ClickHouseClientConfig) field.get(sink);
    }
}
