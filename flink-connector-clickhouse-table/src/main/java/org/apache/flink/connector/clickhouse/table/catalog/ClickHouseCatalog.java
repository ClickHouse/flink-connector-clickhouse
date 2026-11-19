package org.apache.flink.connector.clickhouse.table.catalog;

import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.metadata.TableSchema;

import org.apache.flink.connector.clickhouse.table.schema.SchemaDeriver;
import org.apache.flink.connector.clickhouse.table.schema.SchemaResolverOptions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.util.ExceptionUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.DATABASE;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.TABLE;
import static org.apache.flink.connector.clickhouse.table.catalog.ClickHouseCatalogOptions.DEFAULT_DATABASE;

/**
 * Read-only Flink catalog over a ClickHouse server: lists its databases and tables, and returns
 * each table with a derived Flink schema and the {@code 'connector' = 'clickhouse'} options
 * pre-filled, so that {@code INSERT INTO} needs no per-table DDL.
 *
 * <p>Nothing is cached: every call reads ClickHouse, so a change made there shows at once.
 */
public class ClickHouseCatalog extends AbstractCatalog {

    private static final String LIST_DATABASES = "SELECT name FROM system.databases ORDER BY name";
    private static final String DATABASE_COMMENT =
            "SELECT comment FROM system.databases WHERE name = {database:String}";
    // Temporary tables carry an empty database name, so no listing returns them.
    private static final String LIST_TABLES =
            "SELECT name FROM system.tables WHERE database = {database:String} ORDER BY name";
    private static final String FIND_TABLE =
            "SELECT name FROM system.tables WHERE database = {database:String} AND name = {table:String}";

    /** The server's code for a DESCRIBE whose database is absent; {@link ServerException#TABLE_NOT_FOUND} is the table's. */
    private static final int UNKNOWN_DATABASE = 81;

    /** Every option of a table but its {@code database} and {@code table}. */
    private final Map<String, String> tableOptions;
    private final ClickHouseMetadataClient metadata;
    private final ClickHouseCatalogSinkFactory sinkFactory;

    public ClickHouseCatalog(String name, String defaultDatabase, Map<String, String> tableOptions,
                             ClickHouseMetadataClient metadata, DynamicTableSinkFactory sinkFactory) {
        super(name, defaultDatabase);
        this.tableOptions = Collections.unmodifiableMap(new HashMap<>(tableOptions));
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.sinkFactory = new ClickHouseCatalogSinkFactory(sinkFactory);
    }

    /** Runs at CREATE CATALOG, so a wrong endpoint, credential or default database fails there. */
    @Override
    public void open() throws CatalogException {
        if (!databaseExists(getDefaultDatabase())) {
            throw new ValidationException(String.format(
                    "Default database '%s' of ClickHouse catalog '%s' does not exist — set '%s' to "
                    + "an existing database.",
                    getDefaultDatabase(), getName(), DEFAULT_DATABASE.key()));
        }
    }

    @Override
    public void close() throws CatalogException {
        // Nothing is held between calls.
    }

    // ------------------------------------------------------------------------------------
    // Databases
    // ------------------------------------------------------------------------------------

    @Override
    public List<String> listDatabases() throws CatalogException {
        return metadata.queryColumn(LIST_DATABASES, Collections.emptyMap());
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        return databaseComment(databaseName)
                .map(comment -> new Database(Collections.emptyMap(), comment))
                .orElseThrow(() -> new DatabaseNotExistException(getName(), databaseName));
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return databaseComment(databaseName).isPresent();
    }

    /** Empty when the database does not exist. */
    private Optional<String> databaseComment(String databaseName) {
        List<String> comments =
                metadata.queryColumn(DATABASE_COMMENT, Collections.singletonMap("database", databaseName));
        return comments.isEmpty() ? Optional.empty() : Optional.of(Objects.toString(comments.get(0), ""));
    }

    // ------------------------------------------------------------------------------------
    // Tables
    // ------------------------------------------------------------------------------------

    /** ClickHouse views are listed here too: to Flink they are tables, not expandable queries. */
    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        List<String> tables = metadata.queryColumn(LIST_TABLES, Collections.singletonMap("database", databaseName));
        // Only an empty answer leaves open whether the database exists.
        if (tables.isEmpty() && !databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        return tables;
    }

    @Override
    public List<String> listViews(String databaseName) {
        return Collections.emptyList();
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("database", tablePath.getDatabaseName());
        parameters.put("table", tablePath.getObjectName());
        return !metadata.queryColumn(FIND_TABLE, parameters).isEmpty();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        checkAddressable(tablePath);
        Map<String, String> options = new HashMap<>(tableOptions);
        options.put(DATABASE.key(), tablePath.getDatabaseName());
        options.put(TABLE.key(), tablePath.getObjectName());

        TableSchema clickHouseSchema;
        try {
            clickHouseSchema = metadata.describe(options);
        } catch (RuntimeException e) {
            if (isUnknownTable(e)) {
                throw new TableNotExistException(getName(), tablePath, e);
            }
            throw e;
        }
        Schema schema = Schema.newBuilder()
                .fromResolvedSchema(SchemaDeriver.derive(clickHouseSchema, tablePath.getFullName()))
                .build();
        return new ClickHouseCatalogTable(schema, options);
    }

    /** A name needing quotes would be spliced into the DESCRIBE unquoted and could read another table. */
    private void checkAddressable(ObjectPath tablePath) throws TableNotExistException {
        if (SchemaResolverOptions.isUnquotedName(tablePath.getObjectName())) {
            return;
        }
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }
        throw new ValidationException(String.format(
                "ClickHouse table %s cannot be used through the connector: its name needs quoting "
                + "in SQL, and the ClickHouse client concatenates table names unquoted — only "
                + "names matching [a-zA-Z_][a-zA-Z0-9_]* work.", tablePath.getFullName()));
    }

    private static boolean isUnknownTable(Throwable failure) {
        return ExceptionUtils.findThrowable(failure, ServerException.class)
                .map(ServerException::getCode)
                .filter(code -> code == ServerException.TABLE_NOT_FOUND || code == UNKNOWN_DATABASE)
                .isPresent();
    }

    /** The planner builds the sinks of this catalog's tables through it, so their errors can name the catalog. */
    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(sinkFactory);
    }

    // ------------------------------------------------------------------------------------
    // Partitions, functions and statistics — none here
    // ------------------------------------------------------------------------------------

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters) {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException {
        throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return false;
    }

    @Override
    public List<String> listFunctions(String databaseName) {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) {
        return false;
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath,
                                                                CatalogPartitionSpec partitionSpec) {
        return CatalogColumnStatistics.UNKNOWN;
    }

    // ------------------------------------------------------------------------------------
    // Mutations — refused
    // ------------------------------------------------------------------------------------

    private UnsupportedOperationException readOnly(String statement) {
        return new UnsupportedOperationException(String.format(
                "ClickHouse catalog '%s' is read-only and does not support %s. It mirrors the "
                + "databases and tables of ClickHouse: change those in ClickHouse itself, where "
                + "the catalog sees them immediately, and keep Flink-only objects such as views "
                + "and functions TEMPORARY or in another catalog.",
                getName(), statement));
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists) {
        throw readOnly("CREATE DATABASE");
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) {
        throw readOnly("DROP DATABASE");
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) {
        throw readOnly("ALTER DATABASE");
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) {
        throw readOnly("CREATE TABLE or CREATE VIEW");
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) {
        throw readOnly("DROP TABLE");
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) {
        throw readOnly("ALTER TABLE ... RENAME");
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) {
        throw readOnly("ALTER TABLE");
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
                                CatalogPartition partition, boolean ignoreIfExists) {
        throw readOnly("ALTER TABLE ... ADD PARTITION");
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) {
        throw readOnly("ALTER TABLE ... DROP PARTITION");
    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
                               CatalogPartition newPartition, boolean ignoreIfNotExists) {
        throw readOnly("ALTER TABLE ... PARTITION");
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) {
        throw readOnly("CREATE FUNCTION");
    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) {
        throw readOnly("ALTER FUNCTION");
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) {
        throw readOnly("DROP FUNCTION");
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics,
                                     boolean ignoreIfNotExists) {
        throw readOnly("ANALYZE TABLE");
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics,
                                           boolean ignoreIfNotExists) {
        throw readOnly("ANALYZE TABLE");
    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
                                         CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists) {
        throw readOnly("ANALYZE TABLE");
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
                                               CatalogColumnStatistics columnStatistics,
                                               boolean ignoreIfNotExists) {
        throw readOnly("ANALYZE TABLE");
    }

    /** flink-table-common ships only the interface. */
    private static final class Database implements CatalogDatabase {

        private final Map<String, String> properties;
        private final String comment;

        private Database(Map<String, String> properties, String comment) {
            this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
            this.comment = comment;
        }

        @Override
        public Map<String, String> getProperties() {
            return properties;
        }

        @Override
        public String getComment() {
            return comment;
        }

        @Override
        public CatalogDatabase copy() {
            return this;
        }

        @Override
        public CatalogDatabase copy(Map<String, String> newProperties) {
            return new Database(newProperties, comment);
        }

        @Override
        public Optional<String> getDescription() {
            return Optional.of(comment);
        }

        @Override
        public Optional<String> getDetailedDescription() {
            return Optional.empty();
        }
    }
}
