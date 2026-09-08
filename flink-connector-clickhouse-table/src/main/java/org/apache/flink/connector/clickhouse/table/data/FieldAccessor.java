package org.apache.flink.connector.clickhouse.table.data;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.Objects;

/**
 * Serializable pair of a positional {@link RowData.FieldGetter} and a {@link ValueConverter}:
 * reads one field of a {@link RowData} and converts it to the plain Java value its ClickHouse
 * column expects. A {@code null} field stays {@code null} — schema resolution has already
 * guaranteed the target column is {@code Nullable}.
 */
public final class FieldAccessor implements Serializable {
    private static final long serialVersionUID = 1L;

    private final RowData.FieldGetter fieldGetter;
    private final ValueConverter converter;

    private FieldAccessor(RowData.FieldGetter fieldGetter, ValueConverter converter) {
        this.fieldGetter = Objects.requireNonNull(fieldGetter, "fieldGetter");
        this.converter = Objects.requireNonNull(converter, "converter");
    }

    public static FieldAccessor of(RowData.FieldGetter fieldGetter, ValueConverter converter) {
        return new FieldAccessor(fieldGetter, converter);
    }

    /** Extracts the field's plain Java value from the row, or {@code null}. */
    public Object get(RowData row) {
        Object value = fieldGetter.getFieldOrNull(row);
        return value == null ? null : converter.convert(value);
    }
}
