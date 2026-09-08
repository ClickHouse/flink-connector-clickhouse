package org.apache.flink.connector.clickhouse.table.schema;

import com.clickhouse.data.ClickHouseColumn;

import org.apache.flink.connector.clickhouse.table.data.FieldAccessor;

import java.util.Objects;

/**
 * One resolved column of the sink: the introspected ClickHouse column (wrappers intact — its
 * {@code getOriginalTypeName()} is the canonical header type expression) and the accessor that
 * extracts and converts the physical Flink field's value.
 *
 * <p>Planning-time only; {@code RowDataDataMapper} extracts the serializable parts.
 */
public final class ResolvedColumnMapping {

    public final ClickHouseColumn column;
    public final FieldAccessor accessor;

    public ResolvedColumnMapping(ClickHouseColumn column, FieldAccessor accessor) {
        this.column = Objects.requireNonNull(column, "column");
        this.accessor = Objects.requireNonNull(accessor, "accessor");
    }

    /** The ClickHouse column name — also the Flink field name and the payload map key. */
    public String columnName() {
        return column.getColumnName();
    }

    /** The canonical type expression the header writer emits for this column. */
    public String typeExpression() {
        return column.getOriginalTypeName();
    }
}
