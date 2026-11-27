package org.apache.flink.connector.clickhouse.table.schema;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;

import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.ArrayList;
import java.util.List;

import static com.clickhouse.utils.writer.DataWriter.unwrapSimpleAggregateFunction;

/**
 * The inverse of {@link SchemaResolver}, for the catalog: the Flink schema of a ClickHouse table,
 * which resolves against the columns it was derived from — except under
 * {@code sink.strict-numeric-mapping}, which no Flink type passes for an unsigned column.
 *
 * <p>MATERIALIZED, ALIAS and EPHEMERAL columns are left out, since no insert can carry them; a
 * column the sink cannot write fails the whole table, naming the column.
 */
public final class SchemaDeriver {

    private SchemaDeriver() {}

    public static ResolvedSchema derive(TableSchema clickHouseSchema, String qualifiedTable) {
        List<Column> columns = new ArrayList<>();
        for (ClickHouseColumn column : clickHouseSchema.getColumns()) {
            if (!ServerComputedColumns.isInsertTarget(column)) {
                continue;
            }
            checkNotReservedName(column, qualifiedTable);
            columns.add(Column.physical(column.getColumnName(),
                    TypeConversions.fromLogicalToDataType(flinkType(column, qualifiedTable))));
        }
        return ResolvedSchema.of(columns);
    }

    /** {@link SchemaResolver} refuses the name as a sink column, so no derived schema may carry it. */
    private static void checkNotReservedName(ClickHouseColumn column, String qualifiedTable) {
        if (ClickHousePayload.RAW_KEY.equals(column.getColumnName())) {
            throw new ValidationException(String.format(
                    "ClickHouse column '%s' of %s collides with the connector's reserved payload key, so the "
                    + "catalog cannot expose the table. Declare the table with CREATE TEMPORARY TABLE "
                    + "and leave the column out to let the server default apply.",
                    column.getColumnName(), qualifiedTable));
        }
    }

    private static LogicalType flinkType(ClickHouseColumn column, String qualifiedTable) {
        LogicalType type;
        try {
            type = ClickHouseTypeMapper.toFlinkType(column);
        } catch (TypeMappingException e) {
            throw new ValidationException(String.format(
                    "ClickHouse column '%s %s' of %s has no Flink type the sink can write (%s), so the "
                    + "catalog cannot expose the table. Declare the table with CREATE TEMPORARY TABLE "
                    + "and leave the column out to let the server default apply.",
                    column.getColumnName(), column.getOriginalTypeName(), qualifiedTable, e.getMessage()), e);
        }
        // The sink cannot write a null to these yet (issue #144), so they are exposed NOT NULL.
        boolean nullUnwritable = SchemaResolver.NULL_HANDLING_BROKEN_TARGETS.contains(
                unwrapSimpleAggregateFunction(column).getDataType());
        return nullUnwritable ? type.copy(false) : type;
    }
}
