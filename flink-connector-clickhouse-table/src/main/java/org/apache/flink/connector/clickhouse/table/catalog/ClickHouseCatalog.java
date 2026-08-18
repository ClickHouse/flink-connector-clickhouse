package org.apache.flink.connector.clickhouse.table.catalog;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.data.ClickHouseColumn;

import org.apache.flink.connector.clickhouse.introspection.TableIntrospector;
import org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions;
import org.apache.flink.connector.clickhouse.table.schema.ClickHouseTypeMapper;
import org.apache.flink.connector.clickhouse.table.schema.SchemaResolver;
import org.apache.flink.connector.clickhouse.table.schema.TypeMappingException;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Read-only Flink catalog over a ClickHouse server: list / exists / {@code getTable} are
 * real, every mutating operation throws. {@code getTable} introspects the real table
 * (reusing {@link TableIntrospector}'s memo), reverse-maps each column to the Flink type a
 * user would have declared by hand ({@link ClickHouseTypeMapper#toFlinkType}) and returns
 * the schema with {@code 'connector' = 'clickhouse'} plus connection options pre-filled —
 * from there the planner runs the sink pipeline unchanged.
 *
 * <p>Version-agnostic and cross-compiled per Flink generation; the one API that diverges
 * between Flink 1.x and 2.x ({@code CatalogTable} construction) is injected as
 * {@link CatalogTableConstructor} by the per-generation {@code ClickHouseCatalogFactory}.
 */
public class ClickHouseCatalog extends AbstractCatalog {

    /** Builds the generation-specific {@code CatalogTable} ({@code of()} on 1.x, builder on 2.x). */
    @FunctionalInterface
    public interface CatalogTableConstructor {
        CatalogTable construct(Schema schema, Map<String, String> options);
    }

    /** The sink factory identifier — duplicated here because the factory class is per-generation. */
    private static final String CONNECTOR_IDENTIFIER = "clickhouse";

    private static final String VIEW_ENGINES = "'View', 'MaterializedView', 'LiveView', 'WindowView'";

    private final String url;
    private final String username;
    private final String password;
    private final Map<String, String> forwardedTableOptions;
    private final CatalogTableConstructor catalogTableConstructor;

    private Client client;

    public ClickHouseCatalog(String name, String defaultDatabase, String url, String username,
                             String password, Map<String, String> forwardedTableOptions,
                             CatalogTableConstructor catalogTableConstructor) {
        super(name, defaultDatabase);
        this.url = Objects.requireNonNull(url, "url");
        this.username = Objects.requireNonNull(username, "username");
        this.password = Objects.requireNonNull(password, "password");
        this.forwardedTableOptions = new HashMap<>(
                Objects.requireNonNull(forwardedTableOptions, "forwardedTableOptions"));
        this.catalogTableConstructor =
                Objects.requireNonNull(catalogTableConstructor, "catalogTableConstructor");
    }

    // ------------------------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------------------------

    @Override
    public void open() throws CatalogException {
        if (client != null) {
            return;
        }
        Client candidate = new Client.Builder()
                .addEndpoint(url)
                .setUsername(username)
                .setPassword(password)
                .setOptions(strippedPrefixOptions(ClickHouseConnectorOptions.CLIENT_OPTIONS_PREFIX))
                .build();
        try {
            // Not ping(): the ping endpoint skips authentication, a query validates it too.
            candidate.queryAll("SELECT 1");
        } catch (Exception e) {
            candidate.close();
            throw new CatalogException(String.format(
                    "Could not connect to ClickHouse at %s as '%s' — %s",
                    url, username, e.getMessage()), e);
        }
        client = candidate;
        checkDefaultDatabaseExists();
    }

    private void checkDefaultDatabaseExists() {
        if (!databaseExists(getDefaultDatabase())) {
            close();
            throw new CatalogException(String.format(
                    "The configured default-database '%s' does not exist in ClickHouse at %s.",
                    getDefaultDatabase(), url));
        }
    }

    @Override
    public void close() throws CatalogException {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    /** {@code clickhouse.client.<key>} catalog options also configure the catalog's own client. */
    private Map<String, String> strippedPrefixOptions(String prefix) {
        Map<String, String> stripped = new HashMap<>();
        forwardedTableOptions.forEach((key, value) -> {
            if (key.startsWith(prefix)) {
                stripped.put(key.substring(prefix.length()), value);
            }
        });
        return stripped;
    }

    // ------------------------------------------------------------------------------------
    // Databases
    // ------------------------------------------------------------------------------------

    @Override
    public List<String> listDatabases() throws CatalogException {
        return names(metadataQuery("SELECT name FROM system.databases ORDER BY name",
                Collections.emptyMap()));
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return !metadataQuery("SELECT 1 FROM system.databases WHERE name = {database:String}",
                Map.of("database", databaseName)).isEmpty();
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        return new ClickHouseCatalogDatabase();
    }

    // ------------------------------------------------------------------------------------
    // Tables
    // ------------------------------------------------------------------------------------

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        checkDatabaseExists(databaseName);
        return names(metadataQuery(
                "SELECT name FROM system.tables WHERE database = {database:String} ORDER BY name",
                Map.of("database", databaseName)));
    }

    @Override
    public List<String> listViews(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        checkDatabaseExists(databaseName);
        return names(metadataQuery(
                "SELECT name FROM system.tables WHERE database = {database:String} "
                        + "AND engine IN (" + VIEW_ENGINES + ") ORDER BY name",
                Map.of("database", databaseName)));
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        return !metadataQuery(
                "SELECT 1 FROM system.tables WHERE database = {database:String} AND name = {table:String}",
                Map.of("database", tablePath.getDatabaseName(),
                        "table", tablePath.getObjectName())).isEmpty();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }
        return catalogTableConstructor.construct(
                flinkSchema(tablePath, introspect(tablePath)), connectorOptions(tablePath));
    }

    /** Memoized with the sink factory's lookups — planning re-uses the schema read here. */
    private TableSchema introspect(ObjectPath tablePath) {
        ensureOpen();
        try {
            return TableIntrospector.introspect(
                    url, tablePath.getDatabaseName(), tablePath.getObjectName(), () -> client);
        } catch (Exception e) {
            throw new CatalogException(String.format(
                    "Could not read the schema of ClickHouse table %s at %s — %s",
                    tablePath.getFullName(), url, e.getMessage()), e);
        }
    }

    private static Schema flinkSchema(ObjectPath tablePath, TableSchema clickHouseSchema) {
        Schema.Builder schema = Schema.newBuilder();
        for (ClickHouseColumn column : clickHouseSchema.getColumns()) {
            // MATERIALIZED/ALIAS/EPHEMERAL cannot be inserted into; the sink's resolver
            // rejects them, so a sink-only catalog leaves them out.
            if (SchemaResolver.isServerComputed(column)) {
                continue;
            }
            try {
                schema.column(column.getColumnName(), ClickHouseTypeMapper.toFlinkType(column));
            } catch (TypeMappingException e) {
                throw new CatalogException(String.format(
                        "Column '%s %s' of ClickHouse table %s has no Flink counterpart (%s). "
                        + "Declare the table by hand with CREATE TEMPORARY TABLE, omitting the column.",
                        column.getColumnName(), column.getOriginalTypeName(),
                        tablePath.getFullName(), e.getMessage()), e);
            }
        }
        return schema.build();
    }

    private Map<String, String> connectorOptions(ObjectPath tablePath) {
        Map<String, String> options = new HashMap<>(forwardedTableOptions);
        options.put(FactoryUtil.CONNECTOR.key(), CONNECTOR_IDENTIFIER);
        options.put(ClickHouseConnectorOptions.URL.key(), url);
        options.put(ClickHouseConnectorOptions.USERNAME.key(), username);
        options.put(ClickHouseConnectorOptions.PASSWORD.key(), password);
        options.put(ClickHouseConnectorOptions.DATABASE.key(), tablePath.getDatabaseName());
        options.put(ClickHouseConnectorOptions.TABLE.key(), tablePath.getObjectName());
        return options;
    }

    // ------------------------------------------------------------------------------------
    // Query plumbing
    // ------------------------------------------------------------------------------------

    private List<GenericRecord> metadataQuery(String sql, Map<String, Object> parameters) {
        ensureOpen();
        try {
            return client.queryAll(sql, parameters);
        } catch (Exception e) {
            throw new CatalogException(String.format(
                    "ClickHouse metadata query failed at %s — %s", url, e.getMessage()), e);
        }
    }

    private void ensureOpen() {
        if (client == null) {
            throw new CatalogException(
                    "Catalog '" + getName() + "' is not open — open() must be called before use.");
        }
    }

    private void checkDatabaseExists(String databaseName) throws DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    private static List<String> names(List<GenericRecord> rows) {
        return rows.stream().map(row -> row.getString("name")).collect(Collectors.toList());
    }

    private static UnsupportedOperationException readOnly(String operation) {
        return new UnsupportedOperationException(String.format(
                "%s is not supported — the ClickHouse catalog is read-only. Apply the DDL in "
                + "ClickHouse itself; the change is visible here immediately.", operation));
    }

    // ------------------------------------------------------------------------------------
    // Mutations — read-only catalog, every one throws
    // ------------------------------------------------------------------------------------

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists) {
        throw readOnly("createDatabase");
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) {
        throw readOnly("dropDatabase");
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) {
        throw readOnly("alterDatabase");
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) {
        throw readOnly("createTable");
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) {
        throw readOnly("alterTable");
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) {
        throw readOnly("dropTable");
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) {
        throw readOnly("renameTable");
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
                                CatalogPartition partition, boolean ignoreIfExists) {
        throw readOnly("createPartition");
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
                              boolean ignoreIfNotExists) {
        throw readOnly("dropPartition");
    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
                               CatalogPartition newPartition, boolean ignoreIfNotExists) {
        throw readOnly("alterPartition");
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function,
                               boolean ignoreIfExists) {
        throw readOnly("createFunction");
    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction,
                              boolean ignoreIfNotExists) {
        throw readOnly("alterFunction");
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) {
        throw readOnly("dropFunction");
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics,
                                     boolean ignoreIfNotExists) {
        throw readOnly("alterTableStatistics");
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath,
                                           CatalogColumnStatistics columnStatistics,
                                           boolean ignoreIfNotExists) {
        throw readOnly("alterTableColumnStatistics");
    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
                                         CatalogTableStatistics partitionStatistics,
                                         boolean ignoreIfNotExists) {
        throw readOnly("alterPartitionStatistics");
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath,
                                               CatalogPartitionSpec partitionSpec,
                                               CatalogColumnStatistics columnStatistics,
                                               boolean ignoreIfNotExists) {
        throw readOnly("alterPartitionColumnStatistics");
    }

    // ------------------------------------------------------------------------------------
    // Surfaces ClickHouse does not map onto — partitions (Flink's sense), functions, stats
    // ------------------------------------------------------------------------------------

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotPartitionedException, CatalogException {
        throw new TableNotPartitionedException(getName(), tablePath);
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath,
                                                     CatalogPartitionSpec partitionSpec)
            throws TableNotPartitionedException, CatalogException {
        throw new TableNotPartitionedException(getName(), tablePath);
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath,
                                                             List<Expression> filters)
            throws TableNotPartitionedException, CatalogException {
        throw new TableNotPartitionedException(getName(), tablePath);
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return false;
    }

    @Override
    public List<String> listFunctions(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        checkDatabaseExists(databaseName);
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath,
                                                         CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath,
                                                                CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    /** ClickHouse databases have no Flink-visible properties; existence is the whole story. */
    private static final class ClickHouseCatalogDatabase implements CatalogDatabase {

        @Override
        public Map<String, String> getProperties() {
            return Collections.emptyMap();
        }

        @Override
        public String getComment() {
            return "";
        }

        @Override
        public CatalogDatabase copy() {
            return this;
        }

        @Override
        public CatalogDatabase copy(Map<String, String> properties) {
            return this;
        }

        @Override
        public Optional<String> getDescription() {
            return Optional.empty();
        }

        @Override
        public Optional<String> getDetailedDescription() {
            return Optional.empty();
        }
    }
}
