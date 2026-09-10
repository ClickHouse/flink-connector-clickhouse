package org.apache.flink.connector.clickhouse.table.schema;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerComputedColumnsTest {

    private static final ResolvedSchema FLINK_SCHEMA = ResolvedSchema.of(
            Column.physical("id", DataTypes.BIGINT().notNull()),
            Column.physical("c", DataTypes.STRING().notNull()));

    /** {@code id Int64, c String} with {@code c} carrying the given default kind. */
    private static ServerComputedColumns columns(ClickHouseColumn.DefaultValue kind) {
        TableSchema schema = new TableSchema(ClickHouseColumn.parse("id Int64, c String"));
        ClickHouseColumn c = schema.getColumnByName("c");
        c.setHasDefault(true);
        c.setDefaultValue(kind);
        return ServerComputedColumns.of(FLINK_SCHEMA, schema);
    }

    private static final Optional<Set<String>> NO_COLUMN_LIST = Optional.empty();

    @Test
    void plainAndDefaultColumnsAreWritable() {
        TableSchema schema = new TableSchema(ClickHouseColumn.parse("id Int64, c String"));
        ClickHouseColumn c = schema.getColumnByName("c");
        c.setHasDefault(true);
        c.setDefaultValue(ClickHouseColumn.DefaultValue.DEFAULT);
        ServerComputedColumns computed = ServerComputedColumns.of(FLINK_SCHEMA, schema);

        assertFalse(computed.contains("id"));
        assertFalse(computed.contains("c"));
        // Nothing to reject, whatever the statement targets.
        computed.checkNotWritten(NO_COLUMN_LIST, true);
        computed.checkNotWritten(Optional.of(Set.of("id", "c")), true);
    }

    /** A column ClickHouse does not have is not ours to classify — resolution reports it. */
    @Test
    void aColumnMissingFromClickHouseIsNotServerComputed() {
        ServerComputedColumns computed = ServerComputedColumns.of(
                FLINK_SCHEMA, new TableSchema(ClickHouseColumn.parse("id Int64")));

        assertFalse(computed.contains("c"));
        computed.checkNotWritten(NO_COLUMN_LIST, true);
    }

    @Test
    void withoutAColumnListEveryDeclaredColumnIsWrittenSoAServerComputedOneFails() {
        ValidationException e = assertThrows(ValidationException.class, () ->
                columns(ClickHouseColumn.DefaultValue.MATERIALIZED).checkNotWritten(NO_COLUMN_LIST, true));

        assertTrue(e.getMessage().contains("Column 'c'"), e.getMessage());
        assertTrue(e.getMessage().contains("is MATERIALIZED"), e.getMessage());
        assertTrue(e.getMessage().contains("the server computes it"), e.getMessage());
        assertTrue(e.getMessage().contains("Omit the column from the INSERT column list"), e.getMessage());
    }

    /** The fix this whole class exists for: the statement leaves the column to the server. */
    @Test
    void aColumnListThatOmitsTheServerComputedColumnPasses() {
        for (ClickHouseColumn.DefaultValue kind : ClickHouseColumn.DefaultValue.values()) {
            columns(kind).checkNotWritten(Optional.of(Set.of("id")), true);
        }
    }

    @Test
    void aColumnListThatNamesTheServerComputedColumnFails() {
        ValidationException e = assertThrows(ValidationException.class, () ->
                columns(ClickHouseColumn.DefaultValue.ALIAS)
                        .checkNotWritten(Optional.of(Set.of("id", "c")), true));

        assertTrue(e.getMessage().contains("is ALIAS"), e.getMessage());
        assertTrue(e.getMessage().contains("Drop the column from the INSERT column list."), e.getMessage());
    }

    /** EPHEMERAL is insertable SQL-wise, but only via a column list the sink's INSERT never sends. */
    @Test
    void ephemeralColumnsNameTheirOwnReason() {
        ValidationException e = assertThrows(ValidationException.class, () ->
                columns(ClickHouseColumn.DefaultValue.EPHEMERAL).checkNotWritten(NO_COLUMN_LIST, true));

        assertTrue(e.getMessage().contains("is EPHEMERAL"), e.getMessage());
        assertTrue(e.getMessage().contains("sends no column list"), e.getMessage());
    }

    /** Flink 1.17 never reports a column list, so suggesting one would send the user nowhere. */
    @Test
    void withoutColumnListSupportTheOnlyFixOfferedIsTheFlinkSchema() {
        ValidationException e = assertThrows(ValidationException.class, () ->
                columns(ClickHouseColumn.DefaultValue.MATERIALIZED).checkNotWritten(NO_COLUMN_LIST, false));

        assertTrue(e.getMessage().endsWith("Exclude the column from the Flink schema."), e.getMessage());
    }
}
