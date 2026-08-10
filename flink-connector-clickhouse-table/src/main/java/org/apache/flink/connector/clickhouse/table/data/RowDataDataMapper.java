package org.apache.flink.connector.clickhouse.table.data;

import com.clickhouse.data.ClickHouseColumn;

import org.apache.flink.connector.clickhouse.convertor.ColumnBinding;
import org.apache.flink.connector.clickhouse.convertor.DataMapper;
import org.apache.flink.connector.clickhouse.table.schema.ResolvedColumnMapping;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The SQL path's {@link DataMapper}: puts one TypeTags-legal Java value per resolved column
 * into the payload map (keyed by the ClickHouse column name), feeding the existing typed
 * sink unchanged — checkpointing, restore-rehydration, retries and metrics all reuse
 * today's code and state version.
 *
 * <p>Serialization constraint (docs/table-api/dld-RowDataDataMapper.md):
 * {@code ClickHouseColumn} has no cross-version stability contract, so the mapper ships
 * {@code (columnName, typeExpression)} string pairs — the canonical expressions the header
 * writer emits — and {@link #bindings()} re-parses them on the TaskManager.
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
