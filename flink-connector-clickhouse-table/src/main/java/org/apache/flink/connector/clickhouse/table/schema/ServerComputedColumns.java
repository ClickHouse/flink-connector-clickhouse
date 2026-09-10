package org.apache.flink.connector.clickhouse.table.schema;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The Flink-declared columns ClickHouse will not take a value for: MATERIALIZED and ALIAS, which
 * the server computes, and EPHEMERAL, which only a SQL column list can carry — and the sink's
 * {@code INSERT INTO t FORMAT RowBinaryWithNamesAndTypes} has none, so a header naming one is
 * silently dropped.
 *
 * <p>Declaring such a column is not an error by itself: an {@code INSERT INTO t (...)} that leaves
 * it out is legal, because the sink drops the omitted columns from the header and the server fills
 * them. So {@link SchemaResolver} skips them rather than failing, and the rejection waits here
 * until the statement's column list is known — which is only in
 * {@code ClickHouseDynamicTableSink#getSinkRuntimeProvider}.
 */
public final class ServerComputedColumns {

    /** Flink column name to ClickHouse column, in Flink schema order so errors are deterministic. */
    private final Map<String, ClickHouseColumn> columns;

    private ServerComputedColumns(Map<String, ClickHouseColumn> columns) {
        this.columns = Collections.unmodifiableMap(Objects.requireNonNull(columns, "columns"));
    }

    /** The declared physical columns of {@code flinkSchema} the server owns; unknown ones are not ours. */
    public static ServerComputedColumns of(ResolvedSchema flinkSchema, TableSchema clickHouseSchema) {
        Map<String, ClickHouseColumn> clickHouseColumns = SchemaResolver.columnsByName(clickHouseSchema);
        Map<String, ClickHouseColumn> computed = new LinkedHashMap<>();
        for (RowType.RowField field : SchemaResolver.physicalRowType(flinkSchema).getFields()) {
            ClickHouseColumn column = clickHouseColumns.get(field.getName());
            if (column != null && !isInsertTarget(column)) {
                computed.put(field.getName(), column);
            }
        }
        return new ServerComputedColumns(computed);
    }

    public boolean contains(String flinkColumnName) {
        return columns.containsKey(flinkColumnName);
    }

    /**
     * Fails when the statement would write one of these columns. An empty {@code targeted} means
     * the statement carried no column list, so every declared column is written and any of them
     * is an error; otherwise only the names the list carries are.
     *
     * <p>{@code columnListReachesTheSink} is false on Flink 1.17, which never reports a column
     * list — suggesting one there would send the user down a path that cannot work.
     */
    public void checkNotWritten(Optional<Set<String>> targeted, boolean columnListReachesTheSink) {
        for (Map.Entry<String, ClickHouseColumn> entry : columns.entrySet()) {
            if (targeted.isPresent() && !targeted.get().contains(entry.getKey())) {
                continue;
            }
            throw notWritable(entry.getKey(), entry.getValue(),
                    targeted.isPresent(), columnListReachesTheSink);
        }
    }

    private static boolean isInsertTarget(ClickHouseColumn column) {
        return !column.hasDefault() || column.getDefaultValue() == null
                || column.getDefaultValue() == ClickHouseColumn.DefaultValue.DEFAULT;
    }

    private static ValidationException notWritable(String name, ClickHouseColumn column,
                                                   boolean namedByAColumnList,
                                                   boolean columnListReachesTheSink) {
        return new ValidationException(String.format(
                "Column '%s': ClickHouse column '%s %s' is %s — %s. %s",
                name, column.getColumnName(), column.getOriginalTypeName(),
                column.getDefaultValue(), reason(column.getDefaultValue()),
                fix(namedByAColumnList, columnListReachesTheSink)));
    }

    private static String reason(ClickHouseColumn.DefaultValue kind) {
        return kind == ClickHouseColumn.DefaultValue.EPHEMERAL
                ? "the sink's INSERT sends no column list, so a header naming it would be "
                  + "silently dropped"
                : "the server computes it, so nothing may be sent";
    }

    private static String fix(boolean namedByAColumnList, boolean columnListReachesTheSink) {
        if (namedByAColumnList) {
            return "Drop the column from the INSERT column list.";
        }
        return columnListReachesTheSink
                ? "Omit the column from the INSERT column list, or exclude it from the Flink schema."
                : "Exclude the column from the Flink schema.";
    }
}
