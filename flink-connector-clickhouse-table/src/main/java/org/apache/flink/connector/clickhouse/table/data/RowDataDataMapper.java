package org.apache.flink.connector.clickhouse.table.data;

import com.clickhouse.data.ClickHouseColumn;

import org.apache.flink.connector.clickhouse.convertor.ColumnBinding;
import org.apache.flink.connector.clickhouse.convertor.DataMapper;
import org.apache.flink.connector.clickhouse.table.schema.ResolvedColumnMapping;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The SQL path's {@link DataMapper}: puts one TypeTags-legal Java value per resolved column into
 * the payload map, keyed by ClickHouse column name, leaving the state format untouched.
 *
 * <p>{@code ClickHouseColumn} has no serialization contract, so the mapper ships
 * {@code (columnName, typeExpression)} string pairs and {@link #bindings()} re-parses them on
 * the TaskManager.
 */
public class RowDataDataMapper extends DataMapper<RowData> {
    private static final long serialVersionUID = 1L;

    private final String[] columnNames;
    private final String[] typeExpressions;
    private final FieldAccessor[] accessors;

    private RowDataDataMapper(String[] columnNames, String[] typeExpressions,
                              FieldAccessor[] accessors) {
        this.columnNames = columnNames;
        this.typeExpressions = typeExpressions;
        this.accessors = accessors;
    }

    /** Builds the mapper from the planning-time schema resolution result. */
    public static RowDataDataMapper of(List<ResolvedColumnMapping> mappings) {
        String[] columnNames = new String[mappings.size()];
        String[] typeExpressions = new String[mappings.size()];
        FieldAccessor[] accessors = new FieldAccessor[mappings.size()];
        for (int i = 0; i < mappings.size(); i++) {
            ResolvedColumnMapping mapping = mappings.get(i);
            columnNames[i] = mapping.columnName();
            typeExpressions[i] = mapping.typeExpression();
            accessors[i] = mapping.accessor;
        }
        return new RowDataDataMapper(columnNames, typeExpressions, accessors);
    }

    /**
     * A mapper over {@code keptColumns} only, in this mapper's column order — the columns an
     * {@code INSERT INTO t (...)} left out stay out of both the header and the row, so the server
     * applies their DEFAULT instead of receiving the planner's padding null. Accessors keep their
     * physical row index: the row itself is still full width.
     */
    public RowDataDataMapper project(Set<String> keptColumns) {
        List<Integer> kept = new ArrayList<>(keptColumns.size());
        for (int i = 0; i < columnNames.length; i++) {
            if (keptColumns.contains(columnNames[i])) {
                kept.add(i);
            }
        }
        if (kept.isEmpty()) {
            throw new ValidationException(String.format(
                    "The INSERT column list %s names no column this sink writes (it writes %s) — "
                    + "nothing would be inserted.",
                    keptColumns, Arrays.toString(columnNames)));
        }
        String[] names = new String[kept.size()];
        String[] types = new String[kept.size()];
        FieldAccessor[] getters = new FieldAccessor[kept.size()];
        for (int i = 0; i < kept.size(); i++) {
            int source = kept.get(i);
            names[i] = columnNames[source];
            types[i] = typeExpressions[source];
            getters[i] = accessors[source];
        }
        return new RowDataDataMapper(names, types, getters);
    }

    @Override
    public void toMap(RowData row, Map<String, Object> map) {
        for (int i = 0; i < columnNames.length; i++) {
            map.put(columnNames[i], accessors[i].get(row));
        }
    }

    @Override
    public List<ColumnBinding> bindings() {
        List<ColumnBinding> bindings = new ArrayList<>(columnNames.length);
        for (int i = 0; i < columnNames.length; i++) {
            bindings.add(ColumnBinding.of(
                    columnNames[i], columnNames[i],
                    ClickHouseColumn.of(columnNames[i], typeExpressions[i])));
        }
        return bindings;
    }
}
