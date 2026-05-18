package org.apache.flink.connector.clickhouse.convertor;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ColumnBindingTest {

    @Test void scalarBindingBasics() {
        ColumnBinding b = ColumnBinding.scalar("my_key", "ch_col", ClickHouseDataType.Int32);
        assertEquals("my_key", b.mapKey);
        assertEquals("ch_col", b.columnName);
        assertEquals(ClickHouseDataType.Int32, b.column.getDataType());
        assertFalse(b.column.isNullable());
        assertFalse(b.column.isLowCardinality());
    }

    @Test void scalarBindingWithNullableAndLowCard() {
        ColumnBinding b = ColumnBinding.scalar("k", "c", ClickHouseDataType.String, true, true);
        assertTrue(b.column.isNullable());
        assertTrue(b.column.isLowCardinality());
    }

    @Test void decimalBindingCarriesPrecisionScale() {
        ColumnBinding b = ColumnBinding.decimal("k", "c", 10, 5);
        assertEquals(ClickHouseDataType.Decimal, b.column.getDataType());
        assertEquals(10, b.column.getPrecision());
        assertEquals(5, b.column.getScale());
    }

    @Test void fixedStringBindingCarriesLength() {
        ColumnBinding b = ColumnBinding.fixedString("k", "c", 8);
        assertEquals(ClickHouseDataType.FixedString, b.column.getDataType());
        assertEquals(8, b.column.getEstimatedLength());
    }

    @Test void dateTime64BindingCarriesScale() {
        ColumnBinding b = ColumnBinding.dateTime64("k", "c", 6);
        assertEquals(ClickHouseDataType.DateTime64, b.column.getDataType());
        assertEquals(6, b.column.getScale());
    }

    @Test void arrayBindingCarriesElementType() {
        ClickHouseColumn elem = ClickHouseColumn.of("", ClickHouseDataType.String.toString());
        ColumnBinding b = ColumnBinding.array("k", "c", elem);
        assertEquals(ClickHouseDataType.Array, b.column.getDataType());
        assertNotNull(b.column.getNestedColumns());
        assertEquals(1, b.column.getNestedColumns().size());
    }

    @Test void ofWithFullColumn() {
        ClickHouseColumn col = ClickHouseColumn.of("c", ClickHouseDataType.Int64.toString());
        ColumnBinding b = ColumnBinding.of("k", "c", col);
        assertEquals(col, b.column);
    }

    @Test void mapBindingCarriesKeyAndValueTypes() {
        ClickHouseColumn keyCol = ClickHouseColumn.of("", ClickHouseDataType.String.toString());
        ClickHouseColumn valCol = ClickHouseColumn.of("", ClickHouseDataType.UInt32.toString());
        ColumnBinding b = ColumnBinding.map("k", "c", keyCol, valCol);
        assertEquals(ClickHouseDataType.Map, b.column.getDataType());
        assertNotNull(b.column.getNestedColumns());
        assertEquals(2, b.column.getNestedColumns().size());
        assertEquals(ClickHouseDataType.String, b.column.getNestedColumns().get(0).getDataType());
        assertEquals(ClickHouseDataType.UInt32, b.column.getNestedColumns().get(1).getDataType());
    }

    @Test void tupleBindingCarriesAllElements() {
        ColumnBinding b = ColumnBinding.tuple("k", "c",
            ClickHouseColumn.of("", ClickHouseDataType.String.toString()),
            ClickHouseColumn.of("", ClickHouseDataType.Int64.toString()),
            ClickHouseColumn.of("", ClickHouseDataType.Bool.toString()));
        assertEquals(ClickHouseDataType.Tuple, b.column.getDataType());
        assertNotNull(b.column.getNestedColumns());
        assertEquals(3, b.column.getNestedColumns().size());
        assertEquals(ClickHouseDataType.String, b.column.getNestedColumns().get(0).getDataType());
        assertEquals(ClickHouseDataType.Int64,  b.column.getNestedColumns().get(1).getDataType());
        assertEquals(ClickHouseDataType.Bool,   b.column.getNestedColumns().get(2).getDataType());
    }

    @Test void equalsAndHashCodeDerivedFromFields() {
        ColumnBinding a = ColumnBinding.scalar("k", "c", ClickHouseDataType.Int32);
        ColumnBinding b = ColumnBinding.scalar("k", "c", ClickHouseDataType.Int32);
        ColumnBinding c = ColumnBinding.scalar("k", "c2", ClickHouseDataType.Int32);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertTrue(!a.equals(c));
    }
}
