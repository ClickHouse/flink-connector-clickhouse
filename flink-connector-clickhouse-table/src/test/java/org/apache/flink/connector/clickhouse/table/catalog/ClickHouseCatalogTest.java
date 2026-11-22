package org.apache.flink.connector.clickhouse.table.catalog;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The catalog over an in-memory stand-in for ClickHouse; the real server is the integration tests' part. */
class ClickHouseCatalogTest {

    private static final Map<String, String> TABLE_OPTIONS = Map.of(
            "connector", "clickhouse",
            "url", "http://localhost:8123",
            "username", "default",
            "sink.max-retries", "3");

    private static final ObjectPath EVENTS = new ObjectPath("analytics", "events");

    private static final DynamicTableSinkFactory NO_SINK =
            new FailingSinkFactory(new ValidationException("no sink is created in this test"));

    /** What the sink's schema resolution says of a SMALLINT derived for a UInt8 column under strict mapping. */
    private static final String STRICT_REJECTION =
            "Column 'small': Flink type SMALLINT NOT NULL cannot be written to ClickHouse column 'small UInt8' — "
            + "values outside the UInt8 range 0..255 can only be caught per record, which "
            + "'sink.strict-numeric-mapping' forbids.";

    /** {@code default} is empty; {@code analytics} holds {@code events} and a table whose name needs quoting. */
    private static FakeClickHouse clickHouse() {
        FakeClickHouse clickHouse = new FakeClickHouse();
        clickHouse.database("default", "");
        clickHouse.database("analytics", "product events");
        clickHouse.table("analytics", "events", "id Int64, name Nullable(String), tags Array(String)");
        clickHouse.table("analytics", "daily-rollup", "id Int64");
        return clickHouse;
    }

    private static ClickHouseCatalog catalog(FakeClickHouse clickHouse) {
        return new ClickHouseCatalog("ch", "default", TABLE_OPTIONS, clickHouse, NO_SINK);
    }

    @Test
    void listsTheDatabasesAndTablesClickHouseReports() throws Exception {
        ClickHouseCatalog catalog = catalog(clickHouse());

        assertEquals(List.of("default", "analytics"), catalog.listDatabases());
        assertEquals(List.of("events", "daily-rollup"), catalog.listTables("analytics"));
        assertEquals(List.of(), catalog.listTables("default"));
        assertTrue(catalog.databaseExists("analytics"));
        assertFalse(catalog.databaseExists("nope"));
        assertTrue(catalog.tableExists(EVENTS));
        assertFalse(catalog.tableExists(new ObjectPath("analytics", "nope")));
        assertFalse(catalog.tableExists(new ObjectPath("nope", "events")));
    }

    /** Tables in the answer already show that the database exists. */
    @Test
    void listingTablesAsksOnceUnlessTheAnswerIsEmpty() throws Exception {
        FakeClickHouse clickHouse = clickHouse();
        ClickHouseCatalog catalog = catalog(clickHouse);

        catalog.listTables("analytics");
        assertEquals(1, clickHouse.queries);

        catalog.listTables("default");
        assertEquals(3, clickHouse.queries);
    }

    @Test
    void anUnknownDatabaseIsReportedAsMissing() {
        ClickHouseCatalog catalog = catalog(clickHouse());

        assertThrows(DatabaseNotExistException.class, () -> catalog.listTables("nope"));
        assertThrows(DatabaseNotExistException.class, () -> catalog.getDatabase("nope"));
    }

    @Test
    void aDatabaseCarriesItsClickHouseComment() throws Exception {
        assertEquals("product events", catalog(clickHouse()).getDatabase("analytics").getComment());
    }

    @Test
    void openRequiresTheDefaultDatabaseToExist() {
        FakeClickHouse clickHouse = clickHouse();
        catalog(clickHouse).open();

        ClickHouseCatalog misconfigured = new ClickHouseCatalog("ch", "staging", TABLE_OPTIONS, clickHouse, NO_SINK);
        ValidationException e = assertThrows(ValidationException.class, misconfigured::open);
        assertTrue(e.getMessage().contains("'staging'"), e.getMessage());
        assertTrue(e.getMessage().contains("'default-database'"), e.getMessage());
    }

    @Test
    void aTableComesWithItsDerivedSchemaAndThePreFilledSinkOptions() throws Exception {
        CatalogBaseTable table = catalog(clickHouse()).getTable(EVENTS);

        assertEquals(CatalogBaseTable.TableKind.TABLE, table.getTableKind());
        assertEquals(Schema.newBuilder()
                        .column("id", DataTypes.BIGINT().notNull())
                        .column("name", DataTypes.STRING())
                        .column("tags", DataTypes.ARRAY(DataTypes.STRING().notNull()).notNull())
                        .build(),
                table.getUnresolvedSchema());

        Map<String, String> expected = new LinkedHashMap<>(TABLE_OPTIONS);
        expected.put("database", "analytics");
        expected.put("table", "events");
        assertEquals(expected, table.getOptions());
    }

    /** The describe is asked for the table through the very options the planner will hand the sink factory. */
    @Test
    void theTableIsDescribedThroughTheOptionsItIsReturnedWith() throws Exception {
        FakeClickHouse clickHouse = clickHouse();

        CatalogBaseTable table = catalog(clickHouse).getTable(EVENTS);

        assertEquals(List.of(table.getOptions()), clickHouse.described);
    }

    /** Nothing is cached, so an ALTER in ClickHouse shows on the next call. */
    @Test
    void everyGetTableReadsTheCurrentSchema() throws Exception {
        FakeClickHouse clickHouse = clickHouse();
        ClickHouseCatalog catalog = catalog(clickHouse);
        CatalogBaseTable before = catalog.getTable(EVENTS);

        clickHouse.table("analytics", "events", "id Int64");

        assertNotEquals(before, catalog.getTable(EVENTS));
        assertEquals(Schema.newBuilder().column("id", DataTypes.BIGINT().notNull()).build(),
                catalog.getTable(EVENTS).getUnresolvedSchema());
    }

    /** The planner applies OPTIONS hints through copy(Map). */
    @Test
    void aCopyWithNewOptionsKeepsTheSchema() throws Exception {
        CatalogTable table = (CatalogTable) catalog(clickHouse()).getTable(EVENTS);

        CatalogTable hinted = table.copy(Map.of("connector", "clickhouse", "sink.max-retries", "0"));

        assertEquals(table.getUnresolvedSchema(), hinted.getUnresolvedSchema());
        assertEquals(Map.of("connector", "clickhouse", "sink.max-retries", "0"), hinted.getOptions());
        assertEquals(table, table.copy(table.getOptions()));
        assertEquals(table, table.copy());
    }

    @Test
    void aMissingTableOrDatabaseIsReportedAsTableNotExist() {
        ClickHouseCatalog catalog = catalog(clickHouse());

        assertThrows(TableNotExistException.class, () -> catalog.getTable(new ObjectPath("analytics", "nope")));
        assertThrows(TableNotExistException.class, () -> catalog.getTable(new ObjectPath("nope", "events")));
    }

    /** Only the server's "unknown table" answers mean absence; any other failure must stay visible. */
    @Test
    void anyOtherDescribeFailureIsPassedOn() {
        FakeClickHouse clickHouse = clickHouse();
        clickHouse.describeFailure = describeFailure(241, "Memory limit exceeded");

        ValidationException e = assertThrows(ValidationException.class, () -> catalog(clickHouse).getTable(EVENTS));
        assertSame(clickHouse.describeFailure, e);
        assertEquals(0, clickHouse.queries);
    }

    /** The planner asks for names like default_catalog as tables, and a user granted single tables is denied those. */
    @Test
    void aDeniedTableTheUserCannotSeeIsReportedAsMissing() {
        FakeClickHouse clickHouse = clickHouse();
        clickHouse.describeFailure = describeFailure(497, "Not enough privileges");
        ClickHouseCatalog catalog = catalog(clickHouse);

        assertThrows(TableNotExistException.class,
                () -> catalog.getTable(new ObjectPath("analytics", "default_catalog")));
        // system.tables lists events, so the table exists and the denial is the error to show.
        assertSame(clickHouse.describeFailure,
                assertThrows(ValidationException.class, () -> catalog.getTable(EVENTS)));
    }

    /** client-v2 would splice the name unquoted into the DESCRIBE, where 'a.b' reads table b of database a. */
    @Test
    void aTableNameNeedingQuotesIsRefusedWithoutBeingDescribed() {
        FakeClickHouse clickHouse = clickHouse();
        ClickHouseCatalog catalog = catalog(clickHouse);

        ValidationException e = assertThrows(ValidationException.class,
                () -> catalog.getTable(new ObjectPath("analytics", "daily-rollup")));
        assertTrue(e.getMessage().contains("analytics.daily-rollup"), e.getMessage());
        assertTrue(e.getMessage().contains("needs quoting"), e.getMessage());
        assertThrows(TableNotExistException.class, () -> catalog.getTable(new ObjectPath("analytics", "no-such")));
        assertEquals(List.of(), clickHouse.described);
    }

    @Test
    void aColumnTheSinkCannotWriteFailsGetTableNamingIt() {
        FakeClickHouse clickHouse = clickHouse();
        clickHouse.table("analytics", "events", "id Int64, status Enum8('new' = 1, 'done' = 2)");

        ValidationException e = assertThrows(ValidationException.class, () -> catalog(clickHouse).getTable(EVENTS));
        assertTrue(e.getMessage().contains("'status Enum8('new' = 1, 'done' = 2)'"), e.getMessage());
        assertTrue(e.getMessage().contains("analytics.events"), e.getMessage());
    }

    /** The user declared no Flink type to change, so the rejection names what they can: the option. */
    @Test
    void aStrictMappingRejectionNamesWhatACatalogUserCanChange() {
        ValidationException rejection = new ValidationException(STRICT_REJECTION);

        ValidationException e = assertThrows(ValidationException.class, () -> createSink(rejection));

        assertTrue(e.getMessage().startsWith(STRICT_REJECTION), e.getMessage());
        assertTrue(e.getMessage().contains("remove 'sink.strict-numeric-mapping' from the catalog"), e.getMessage());
        assertTrue(e.getMessage().contains("/*+ OPTIONS('sink.strict-numeric-mapping' = 'false') */"), e.getMessage());
        assertSame(rejection, e.getCause());
    }

    @Test
    void anyOtherSinkFailureIsPassedOn() {
        ValidationException failure = new ValidationException("Could not read the schema of ClickHouse table");

        assertSame(failure, assertThrows(ValidationException.class, () -> createSink(failure)));
    }

    /** Flink's unsupported-option error lists every supported key, the strict option's among them. */
    @Test
    void anOptionErrorThatListsTheStrictOptionIsPassedOn() {
        ValidationException failure = new ValidationException("Unsupported options found for 'clickhouse'.\n\n"
                + "Unsupported options:\n\nsink.buffer-flush.max-row\n\n"
                + "Supported options:\n\nconnector\nsink.strict-numeric-mapping\nurl");

        assertSame(failure, assertThrows(ValidationException.class, () -> createSink(failure)));
    }

    /** Asks a strict catalog's own factory for the sink of a table, as the planner does. */
    private static void createSink(ValidationException sinkFailure) throws Exception {
        Map<String, String> strict = new HashMap<>(TABLE_OPTIONS);
        strict.put("sink.strict-numeric-mapping", "true");
        ClickHouseCatalog catalog =
                new ClickHouseCatalog("ch", "default", strict, clickHouse(), new FailingSinkFactory(sinkFailure));
        ResolvedCatalogTable table = new ResolvedCatalogTable((CatalogTable) catalog.getTable(EVENTS),
                ResolvedSchema.of(Column.physical("id", DataTypes.BIGINT().notNull())));

        DynamicTableSinkFactory factory = (DynamicTableSinkFactory) catalog.getFactory().get();
        factory.createDynamicTableSink(new FactoryUtil.DefaultDynamicTableContext(
                ObjectIdentifier.of("ch", "analytics", "events"), table, Map.of(), new Configuration(),
                ClickHouseCatalogTest.class.getClassLoader(), false));
    }

    /** The planner asks these while resolving any statement, so they must answer rather than fail. */
    @Test
    void thereAreNoViewsFunctionsPartitionsOrStatistics() throws Exception {
        ClickHouseCatalog catalog = catalog(clickHouse());
        ObjectPath function = new ObjectPath("analytics", "my_udf");

        assertEquals(List.of(), catalog.listViews("analytics"));
        assertEquals(List.of(), catalog.listFunctions("analytics"));
        assertFalse(catalog.functionExists(function));
        assertThrows(FunctionNotExistException.class, () -> catalog.getFunction(function));
        assertFalse(catalog.partitionExists(EVENTS, null));
        assertThrows(PartitionNotExistException.class, () -> catalog.getPartition(EVENTS, null));
        assertSame(CatalogTableStatistics.UNKNOWN, catalog.getTableStatistics(EVENTS));
        assertSame(CatalogColumnStatistics.UNKNOWN, catalog.getTableColumnStatistics(EVENTS));
    }

    /** An empty list would let SHOW PARTITIONS answer for a mistyped table name. */
    @Test
    void listingPartitionsReportsTheTableAsMissingOrNotPartitioned() {
        ClickHouseCatalog catalog = catalog(clickHouse());
        ObjectPath missing = new ObjectPath("analytics", "nope");

        assertThrows(TableNotPartitionedException.class, () -> catalog.listPartitions(EVENTS));
        assertThrows(TableNotPartitionedException.class, () -> catalog.listPartitions(EVENTS, null));
        assertThrows(TableNotPartitionedException.class, () -> catalog.listPartitionsByFilter(EVENTS, List.of()));
        assertThrows(TableNotExistException.class, () -> catalog.listPartitions(missing));
        assertThrows(TableNotExistException.class, () -> catalog.listPartitions(missing, null));
        assertThrows(TableNotExistException.class, () -> catalog.listPartitionsByFilter(missing, List.of()));
    }

    @Test
    void everyMutationIsRefusedAsReadOnly() {
        ClickHouseCatalog catalog = catalog(clickHouse());
        List<Executable> mutations = List.of(
                () -> catalog.createDatabase("d", null, false),
                () -> catalog.dropDatabase("analytics", false, true),
                () -> catalog.alterDatabase("analytics", null, false),
                () -> catalog.createTable(EVENTS, null, true),
                () -> catalog.dropTable(EVENTS, true),
                () -> catalog.renameTable(EVENTS, "renamed", false),
                () -> catalog.alterTable(EVENTS, null, false),
                () -> catalog.createPartition(EVENTS, null, null, false),
                () -> catalog.dropPartition(EVENTS, null, false),
                () -> catalog.alterPartition(EVENTS, null, null, false),
                () -> catalog.createFunction(EVENTS, null, false),
                () -> catalog.alterFunction(EVENTS, null, false),
                () -> catalog.dropFunction(EVENTS, false),
                () -> catalog.alterTableStatistics(EVENTS, null, false),
                () -> catalog.alterTableColumnStatistics(EVENTS, null, false),
                () -> catalog.alterPartitionStatistics(EVENTS, null, null, false),
                () -> catalog.alterPartitionColumnStatistics(EVENTS, null, null, false));

        for (Executable mutation : mutations) {
            UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class, mutation);
            assertTrue(e.getMessage().contains("catalog 'ch' is read-only"), e.getMessage());
        }
    }

    /** What the sink factory's introspection throws: client-v2's constant wrapper over the server's answer. */
    private static ValidationException describeFailure(int serverCode, String serverMessage) {
        return new ValidationException("Could not read the schema of ClickHouse table",
                new ClientException("Failed to get table schema",
                        new ServerException(serverCode, serverMessage, 404, "")));
    }

    /** Stands in for the connector's sink factory, which lives in the version modules. */
    private static final class FailingSinkFactory implements DynamicTableSinkFactory {

        private final ValidationException failure;

        private FailingSinkFactory(ValidationException failure) {
            this.failure = failure;
        }

        @Override
        public String factoryIdentifier() {
            return "clickhouse";
        }

        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            return Set.of();
        }

        @Override
        public Set<ConfigOption<?>> optionalOptions() {
            return Set.of();
        }

        @Override
        public DynamicTableSink createDynamicTableSink(Context context) {
            throw failure;
        }
    }

    /** Answers the catalog's queries by the system table they read and the parameters they bind. */
    private static final class FakeClickHouse implements ClickHouseMetadataClient {

        private final Map<String, String> databaseComments = new LinkedHashMap<>();
        private final Map<String, Map<String, String>> tables = new LinkedHashMap<>();
        private final List<Map<String, String>> described = new ArrayList<>();
        private ValidationException describeFailure;
        private int queries;

        void database(String name, String comment) {
            databaseComments.put(name, comment);
            tables.put(name, new LinkedHashMap<>());
        }

        void table(String database, String name, String columns) {
            tables.get(database).put(name, columns);
        }

        @Override
        public List<String> queryColumn(String sql, Map<String, Object> parameters) {
            queries++;
            Object database = parameters.get("database");
            if (sql.contains("system.databases")) {
                if (database == null) {
                    return new ArrayList<>(databaseComments.keySet());
                }
                return databaseComments.containsKey(database) ? List.of(databaseComments.get(database)) : List.of();
            }
            Map<String, String> inDatabase = tables.getOrDefault(database, Map.of());
            Object table = parameters.get("table");
            if (table == null) {
                return new ArrayList<>(inDatabase.keySet());
            }
            return inDatabase.containsKey(table) ? List.of(table.toString()) : List.of();
        }

        @Override
        public TableSchema describe(Map<String, String> tableOptions) {
            described.add(tableOptions);
            if (describeFailure != null) {
                throw describeFailure;
            }
            String database = tableOptions.get("database");
            String table = tableOptions.get("table");
            if (!tables.containsKey(database)) {
                throw describeFailure(81, "Database " + database + " does not exist");
            }
            String columns = tables.get(database).get(table);
            if (columns == null) {
                throw describeFailure(ServerException.TABLE_NOT_FOUND, "Table " + database + "." + table + " does not exist");
            }
            return new TableSchema(ClickHouseColumn.parse(columns));
        }
    }
}
