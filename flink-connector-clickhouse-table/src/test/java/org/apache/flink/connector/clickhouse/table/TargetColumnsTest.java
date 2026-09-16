package org.apache.flink.connector.clickhouse.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class TargetColumnsTest {

    private static final ResolvedSchema SCHEMA = ResolvedSchema.of(
            Column.physical("id", DataTypes.BIGINT().notNull()),
            Column.physical("props", DataTypes.ROW(DataTypes.FIELD("a", DataTypes.STRING()))),
            Column.physical("note", DataTypes.STRING()));

    @BeforeEach
    void requireTargetColumnSupport() {
        assumeTrue(TargetColumns.isSupported(),
                "Flink 1.17 does not report the INSERT column list to the sink");
    }

    /**
     * {@code Context} gained {@code getTargetColumns()} in Flink 1.18, so a compiled stub would
     * not build against the 1.17 floor this module also compiles against.
     */
    private static DynamicTableSink.Context contextTargeting(int[][] paths) {
        return (DynamicTableSink.Context) Proxy.newProxyInstance(
                DynamicTableSink.Context.class.getClassLoader(),
                new Class<?>[] {DynamicTableSink.Context.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getTargetColumns": return Optional.ofNullable(paths);
                        case "toString": return "target-columns-test-context";
                        case "hashCode": return System.identityHashCode(proxy);
                        case "equals": return proxy == args[0];
                        default: throw new UnsupportedOperationException(method.getName());
                    }
                });
    }

    @Test
    void aStatementWithoutAColumnListWritesEveryColumn() {
        assertEquals(Optional.empty(), TargetColumns.resolve(contextTargeting(null), SCHEMA));
        assertEquals(Optional.empty(), TargetColumns.resolve(contextTargeting(new int[0][]), SCHEMA));
    }

    @Test
    void aColumnListResolvesToTheNamesItTargets() {
        Optional<Set<String>> names =
                TargetColumns.resolve(contextTargeting(new int[][] {{2}, {0}}), SCHEMA);

        assertEquals(Optional.of(Set.of("note", "id")), names);
    }

    @Test
    void aFieldInsideAStructuredColumnIsRejected() {
        ValidationException e = assertThrows(ValidationException.class, () ->
                TargetColumns.resolve(contextTargeting(new int[][] {{1, 0}}), SCHEMA));

        assertTrue(e.getMessage().contains("structured column 'props'"), e.getMessage());
        assertTrue(e.getMessage().contains("writes whole columns only"), e.getMessage());
    }

    @Test
    void anIndexOutsideTheSchemaIsRejected() {
        ValidationException e = assertThrows(ValidationException.class, () ->
                TargetColumns.resolve(contextTargeting(new int[][] {{9}}), SCHEMA));

        assertTrue(e.getMessage().contains("outside the sink's schema"), e.getMessage());
    }
}
