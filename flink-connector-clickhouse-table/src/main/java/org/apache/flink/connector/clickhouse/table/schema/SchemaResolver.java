package org.apache.flink.connector.clickhouse.table.schema;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;

import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.connector.clickhouse.table.data.FieldAccessor;
import org.apache.flink.connector.clickhouse.table.data.ValueConverter;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Pure function from {@code (ResolvedSchema, ClickHouse TableSchema, options)} to the
 * ordered {@code List<ResolvedColumnMapping>} driving the typed sink. All planning-time
 * schema errors are raised here as {@link ValidationException}s.
 *
 * <p>Columns match by name, case-sensitively; only physical columns participate; column
 * order is the Flink schema's order.
 */
public final class SchemaResolver {

    private SchemaResolver() {}

    public static List<ResolvedColumnMapping> resolve(ResolvedSchema flinkSchema,
                                                      TableSchema clickHouseSchema,
                                                      SchemaResolverOptions options) {
        Map<String, ClickHouseColumn> clickHouseColumns = columnsByName(clickHouseSchema);

        List<ResolvedColumnMapping> mappings = new ArrayList<>();
        List<RowType.RowField> fields = physicalRowType(flinkSchema).getFields();
        for (int i = 0; i < fields.size(); i++) {
            RowType.RowField field = fields.get(i);
            checkNotReservedName(field.getName());
            ClickHouseColumn column = clickHouseColumns.get(field.getName());
            if (column == null) {
                if (options.ignoreUnknownFlinkColumns) {
                    continue;
                }
                throw unknownFlinkColumn(field.getName(), clickHouseColumns, options);
            }
            mappings.add(resolveColumn(i, field, column, options));
        }

        checkOmittedColumnsHaveDefaults(clickHouseSchema, mappedNames(mappings), options);
        checkNotEmpty(mappings, options);
        return mappings;
    }

    /** True iff any resolved target contains a JSON column — drives the JSON auto-enable. */
    public static boolean targetsJsonColumn(List<ResolvedColumnMapping> mappings) {
        return mappings.stream().anyMatch(m -> containsJson(m.column));
    }

    // ------------------------------------------------------------------------------------
    // Per-column resolution
    // ------------------------------------------------------------------------------------

    private static ResolvedColumnMapping resolveColumn(int fieldIndex, RowType.RowField field,
                                                       ClickHouseColumn column,
                                                       SchemaResolverOptions options) {
        checkInsertable(field.getName(), column);
        ClickHouseColumn effective = ClickHouseTypeMapper.unwrapTransparentWrappers(column);
        checkNullability(field, column, effective);
        ValueConverter converter = converterFor(field, column, options);
        FieldAccessor accessor = FieldAccessor.of(
                RowData.createFieldGetter(field.getType(), fieldIndex), converter);
        return new ResolvedColumnMapping(fieldIndex, field.getType(), column, accessor);
    }

    private static ValueConverter converterFor(RowType.RowField field, ClickHouseColumn column,
                                               SchemaResolverOptions options) {
        try {
            return ClickHouseTypeMapper.converterFor(
                    field.getType(), column, options.sinkTimezone, field.getName());
        } catch (TypeMappingException e) {
            throw asValidationException(e, field, column);
        }
    }

    private static ValidationException asValidationException(TypeMappingException e,
                                                             RowType.RowField field,
                                                             ClickHouseColumn column) {
        if (e.getKind() == TypeMappingException.Kind.TARGET_UNSUPPORTED) {
            return new ValidationException(String.format(
                    "ClickHouse column '%s %s' is not yet supported by the sink (%s). "
                    + "Exclude the column from the Flink schema to let the server default apply.",
                    column.getColumnName(), column.getOriginalTypeName(), e.getMessage()));
        }
        return new ValidationException(String.format(
                "Column '%s': Flink type %s cannot be written to ClickHouse column '%s %s' — %s.",
                field.getName(), field.getType().asSummaryString(),
                column.getColumnName(), column.getOriginalTypeName(), e.getMessage()));
    }

    private static void checkInsertable(String name, ClickHouseColumn column) {
        if (isServerComputed(column)) {
            throw new ValidationException(String.format(
                    "Column '%s': ClickHouse column '%s %s' is %s and cannot be inserted into. "
                    + "Exclude the column from the Flink schema.",
                    name, column.getColumnName(), column.getOriginalTypeName(),
                    column.getDefaultValue()));
        }
    }

    /**
     * MATERIALIZED/ALIAS/EPHEMERAL columns are computed by the server, so nothing may be
     * sent. The resolver rejects them; the read-only catalog uses the same predicate to
     * leave them out of the schemas it emits.
     */
    public static boolean isServerComputed(ClickHouseColumn column) {
        return column.hasDefault() && column.getDefaultValue() != null
                && column.getDefaultValue() != ClickHouseColumn.DefaultValue.DEFAULT;
    }

    private static void checkNullability(RowType.RowField field, ClickHouseColumn column,
                                         ClickHouseColumn effective) {
        if (!field.getType().isNullable()) {
            return;
        }
        checkTargetIsNullable(field, column, effective);
        checkTargetNullHandlingWorks(field, column, effective);
    }

    /** A byte-exact header cannot carry a null, so the target column must be Nullable. */
    private static void checkTargetIsNullable(RowType.RowField field, ClickHouseColumn column,
                                              ClickHouseColumn effective) {
        if (!effective.isNullable()) {
            throw new ValidationException(String.format(
                    "Column '%s': Flink type %s is nullable but ClickHouse column '%s %s' is not "
                    + "Nullable — a null cannot be written byte-exactly. Declare the Flink column "
                    + "NOT NULL or make the ClickHouse column Nullable.",
                    field.getName(), field.getType().asSummaryString(),
                    column.getColumnName(), column.getOriginalTypeName()));
        }
    }

    private static void checkTargetNullHandlingWorks(RowType.RowField field, ClickHouseColumn column,
                                                     ClickHouseColumn effective) {
        if (ClickHouseTypeMapper.NULL_HANDLING_BROKEN_TARGETS.contains(effective.getDataType())) {
            throw new ValidationException(String.format(
                    "Column '%s': nullable Flink type %s cannot be written to ClickHouse column "
                    + "'%s %s' — null handling for Nullable(UInt8/16/32/64) is broken in the current "
                    + "DataWriter (see issue #144). Declare the Flink column NOT NULL until the fix lands.",
                    field.getName(), field.getType().asSummaryString(),
                    column.getColumnName(), column.getOriginalTypeName()));
        }
    }

    /** The payload map key {@code __clickhouse_raw__} is reserved for STRING-mode payloads. */
    private static void checkNotReservedName(String name) {
        if (ClickHousePayload.RAW_KEY.equals(name)) {
            throw new ValidationException(String.format(
                    "Column '%s' collides with the connector's reserved payload key and cannot be "
                    + "used as a sink column. Rename the column.", name));
        }
    }

    // ------------------------------------------------------------------------------------
    // Whole-schema checks
    // ------------------------------------------------------------------------------------

    /**
     * An omitted ClickHouse column gets its server-side DEFAULT — a non-Nullable column
     * without one has nothing to fall back on and is rejected.
     * MATERIALIZED/ALIAS/EPHEMERAL columns are exempt.
     */
    private static void checkOmittedColumnsHaveDefaults(TableSchema clickHouseSchema,
                                                        Set<String> mappedNames,
                                                        SchemaResolverOptions options) {
        for (ClickHouseColumn column : clickHouseSchema.getColumns()) {
            if (mappedNames.contains(column.getColumnName()) || !isRequired(column)) {
                continue;
            }
            throw new ValidationException(String.format(
                    "ClickHouse column '%s %s' in %s.%s is neither Nullable nor has a DEFAULT and "
                    + "is missing from the Flink schema — add the column to the Flink schema or "
                    + "give it a DEFAULT.",
                    column.getColumnName(), column.getOriginalTypeName(),
                    options.database, options.table));
        }
    }

    private static boolean isRequired(ClickHouseColumn column) {
        if (column.hasDefault()) {
            return false;
        }
        return !ClickHouseTypeMapper.unwrapTransparentWrappers(column).isNullable();
    }

    private static void checkNotEmpty(List<ResolvedColumnMapping> mappings,
                                      SchemaResolverOptions options) {
        if (mappings.isEmpty()) {
            throw new ValidationException(String.format(
                    "None of the Flink schema columns map to ClickHouse table %s.%s — nothing to insert.",
                    options.database, options.table));
        }
    }

    private static ValidationException unknownFlinkColumn(String name,
                                                          Map<String, ClickHouseColumn> clickHouseColumns,
                                                          SchemaResolverOptions options) {
        return new ValidationException(String.format(
                "Column '%s' declared in the Flink schema does not exist in %s.%s. "
                + "ClickHouse columns: %s. "
                + "Set 'sink.ignore-unknown-flink-columns' = 'true' to drop it instead.",
                name, options.database, options.table,
                String.join(", ", clickHouseColumns.keySet())));
    }

    // ------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------

    /** Only physical columns participate; computed/metadata columns never reach the sink. */
    private static RowType physicalRowType(ResolvedSchema flinkSchema) {
        return (RowType) flinkSchema.toPhysicalRowDataType().getLogicalType();
    }

    private static Map<String, ClickHouseColumn> columnsByName(TableSchema clickHouseSchema) {
        // LinkedHashMap keeps the table's column order for error messages.
        return clickHouseSchema.getColumns().stream().collect(Collectors.toMap(
                ClickHouseColumn::getColumnName, c -> c,
                (a, b) -> a, LinkedHashMap::new));
    }

    private static Set<String> mappedNames(List<ResolvedColumnMapping> mappings) {
        return mappings.stream().map(ResolvedColumnMapping::columnName).collect(Collectors.toSet());
    }

    private static boolean containsJson(ClickHouseColumn column) {
        if (column.getDataType() == ClickHouseDataType.JSON) {
            return true;
        }
        if (column.hasNestedColumn()) {
            for (ClickHouseColumn nested : column.getNestedColumns()) {
                if (containsJson(nested)) {
                    return true;
                }
            }
        }
        return false;
    }
}
