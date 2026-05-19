package org.apache.flink.connector.clickhouse.convertor;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;

import java.io.Serializable;
import java.util.Objects;

public final class ColumnBinding implements Serializable {
    private static final long serialVersionUID = 1L;

    public final String mapKey;
    public final String columnName;
    public final ClickHouseColumn column;

    private ColumnBinding(String mapKey, String columnName, ClickHouseColumn column) {
        this.mapKey = Objects.requireNonNull(mapKey, "mapKey");
        this.columnName = Objects.requireNonNull(columnName, "columnName");
        this.column = Objects.requireNonNull(column, "column");
    }

    public static ColumnBinding of(String mapKey, String columnName, ClickHouseColumn column) {
        return new ColumnBinding(mapKey, columnName, column);
    }

    public static ColumnBinding scalar(String mapKey, String columnName, ClickHouseDataType type) {
        return scalar(mapKey, columnName, type, false, false);
    }

    public static ColumnBinding scalar(String mapKey, String columnName, ClickHouseDataType type,
                                       boolean nullable, boolean lowCardinality) {
        StringBuilder typeExpr = new StringBuilder();
        if (lowCardinality) typeExpr.append("LowCardinality(");
        if (nullable) typeExpr.append("Nullable(");
        typeExpr.append(type.name());
        if (nullable) typeExpr.append(')');
        if (lowCardinality) typeExpr.append(')');
        return new ColumnBinding(mapKey, columnName,
                ClickHouseColumn.of(columnName, typeExpr.toString()));
    }

    public static ColumnBinding decimal(String mapKey, String columnName, int precision, int scale) {
        return new ColumnBinding(mapKey, columnName,
                ClickHouseColumn.of(columnName, "Decimal(" + precision + ", " + scale + ")"));
    }

    public static ColumnBinding fixedString(String mapKey, String columnName, int length) {
        return new ColumnBinding(mapKey, columnName,
                ClickHouseColumn.of(columnName, "FixedString(" + length + ")"));
    }

    public static ColumnBinding dateTime64(String mapKey, String columnName, int scale) {
        return new ColumnBinding(mapKey, columnName,
                ClickHouseColumn.of(columnName, "DateTime64(" + scale + ")"));
    }

    public static ColumnBinding dateTime64(String mapKey, String columnName, int scale, String timezone) {
        return new ColumnBinding(mapKey, columnName,
                ClickHouseColumn.of(columnName, "DateTime64(" + scale + ", '" + timezone + "')"));
    }

    public static ColumnBinding array(String mapKey, String columnName, ClickHouseColumn element) {
        return new ColumnBinding(mapKey, columnName,
                ClickHouseColumn.of(columnName, "Array(" + element.getOriginalTypeName() + ")"));
    }

    public static ColumnBinding map(String mapKey, String columnName,
                                    ClickHouseColumn key, ClickHouseColumn value) {
        return new ColumnBinding(mapKey, columnName,
                ClickHouseColumn.of(columnName,
                        "Map(" + key.getOriginalTypeName() + ", " + value.getOriginalTypeName() + ")"));
    }

    public static ColumnBinding tuple(String mapKey, String columnName, ClickHouseColumn... elements) {
        StringBuilder sb = new StringBuilder("Tuple(");
        for (int i = 0; i < elements.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(elements[i].getOriginalTypeName());
        }
        sb.append(')');
        return new ColumnBinding(mapKey, columnName,
                ClickHouseColumn.of(columnName, sb.toString()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnBinding)) return false;
        ColumnBinding that = (ColumnBinding) o;
        return mapKey.equals(that.mapKey)
            && columnName.equals(that.columnName)
            && column.equals(that.column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mapKey, columnName, column);
    }

    @Override
    public String toString() {
        return "ColumnBinding{mapKey='" + mapKey + "', columnName='" + columnName
            + "', column=" + column + '}';
    }
}
