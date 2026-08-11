package org.apache.flink.connector.clickhouse.table.schema;

import com.clickhouse.data.ClickHouseColumn;

import org.apache.flink.connector.clickhouse.table.data.FieldAccessor;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Objects;

/**
 * One resolved column of the sink: the physical Flink field, the introspected ClickHouse
 * column it writes to (wrappers intact — its {@code getOriginalTypeName()} is the canonical
 * header type expression), and the accessor that converts the field's value.
 *
 * <p>Planning-time only; {@code RowDataDataMapper} extracts the serializable parts.
 */
public final class ResolvedColumnMapping {

    public final int flinkFieldIndex;
    public final LogicalType flinkType;
    public final ClickHouseColumn column;
    public final FieldAccessor accessor;

    public ResolvedColumnMapping(int flinkFieldIndex, LogicalType flinkType,
                                 ClickHouseColumn column, FieldAccessor accessor) {
        this.flinkFieldIndex = flinkFieldIndex;
        this.flinkType = Objects.requireNonNull(flinkType, "flinkType");
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
