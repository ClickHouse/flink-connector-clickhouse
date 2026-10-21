package org.apache.flink.connector.clickhouse.table;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;

import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Resolves the column list of an {@code INSERT INTO t (a, b) ...} to the column names the sink
 * must write.
 *
 * <p>The planner does not narrow the row for a partial insert: it pads every column the statement
 * left out with null and names the targeted ones on {@link DynamicTableSink.Context}. Writing the
 * padding would turn "leave this column to the server" into an explicit null and defeat the
 * column's {@code DEFAULT}, so the omitted columns are dropped from the
 * RowBinaryWithNamesAndTypes header instead — the writer sends
 * {@code input_format_defaults_for_omitted_fields=1}, so the server fills them.
 *
 * <p>{@code Context#getTargetColumns()} arrived in Flink 1.18 and this class also compiles against
 * the 1.17 floor, hence the reflective lookup. On 1.17 itself the column list never reaches the
 * sink and a partial insert still writes explicit nulls.
 */
public final class TargetColumns {

    private static final Method GET_TARGET_COLUMNS = lookUpAccessor();

    private TargetColumns() {}

    private static Method lookUpAccessor() {
        try {
            return DynamicTableSink.Context.class.getMethod("getTargetColumns");
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    /** True when the running Flink generation reports the statement's column list to the sink. */
    public static boolean isSupported() {
        return GET_TARGET_COLUMNS != null;
    }

    /**
     * The names the {@code INSERT} column list targets, in statement order, or empty when the
     * statement carried no column list and every resolved column must be written.
     */
    public static Optional<Set<String>> resolve(DynamicTableSink.Context context,
                                                ResolvedSchema schema) {
        int[][] paths = read(context);
        if (paths == null || paths.length == 0) {
            return Optional.empty();
        }
        List<String> declared = schema.getColumnNames();
        Set<String> names = new LinkedHashSet<>();
        for (int[] path : paths) {
            names.add(declared.get(checkWholeColumn(path, declared)));
        }
        return Optional.of(names);
    }

    /**
     * A path of more than one element addresses a field inside a structured column; the sink
     * writes whole columns, so a partial one would have to read-modify-write the rest.
     */
    private static int checkWholeColumn(int[] path, List<String> declared) {
        if (path.length == 0 || path[0] < 0 || path[0] >= declared.size()) {
            throw new ValidationException(
                    "The INSERT column list resolved to a column outside the sink's schema "
                    + declared + " — insert without a column list.");
        }
        if (path.length > 1) {
            throw new ValidationException(String.format(
                    "The INSERT column list targets a field inside structured column '%s'; the "
                    + "ClickHouse sink writes whole columns only. Name the whole column, or "
                    + "insert without a column list.",
                    declared.get(path[0])));
        }
        return path[0];
    }

    @SuppressWarnings("unchecked")
    private static int[][] read(DynamicTableSink.Context context) {
        if (GET_TARGET_COLUMNS == null) {
            return null;
        }
        try {
            return ((Optional<int[][]>) GET_TARGET_COLUMNS.invoke(context)).orElse(null);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Could not read the INSERT column list from the planner", e);
        }
    }
}
