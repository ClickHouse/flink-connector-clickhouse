package org.apache.flink.connector.clickhouse.table.data;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.Objects;

/**
 * Reads one field of a {@link RowData} and unwraps it to the plain Java value its
 * ClickHouse column expects: a serializable pair of Flink's positional
 * {@link RowData.FieldGetter} and the pair-chosen {@link ValueConverter}.
 *
 * <p>Values are copied out of the (possibly buffer-reusing) {@code RowData} immediately;
 * a {@code null} field stays {@code null} — schema resolution guarantees nulls only reach
 * ClickHouse {@code Nullable} columns.
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
