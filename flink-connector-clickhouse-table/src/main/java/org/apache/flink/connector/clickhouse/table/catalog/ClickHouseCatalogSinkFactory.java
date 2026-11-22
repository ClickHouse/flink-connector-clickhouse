package org.apache.flink.connector.clickhouse.table.catalog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.SINK_STRICT_NUMERIC_MAPPING;

/**
 * The connector's sink factory as {@link ClickHouseCatalog} hands it to the planner: the same sink,
 * with a strict-mapping rejection that names what the user of a derived schema can change.
 */
final class ClickHouseCatalogSinkFactory implements DynamicTableSinkFactory {

    private final DynamicTableSinkFactory sinkFactory;
    /** False when only an OPTIONS hint can have turned strict mapping on. */
    private final boolean strictCatalog;

    ClickHouseCatalogSinkFactory(DynamicTableSinkFactory sinkFactory, Map<String, String> catalogTableOptions) {
        this.sinkFactory = Objects.requireNonNull(sinkFactory, "sinkFactory");
        this.strictCatalog = isStrict(catalogTableOptions);
    }

    @Override
    public String factoryIdentifier() {
        return sinkFactory.factoryIdentifier();
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return sinkFactory.requiredOptions();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return sinkFactory.optionalOptions();
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return sinkFactory.forwardOptions();
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        try {
            return sinkFactory.createDynamicTableSink(context);
        } catch (ValidationException e) {
            throw withCatalogRemedy(e, context.getCatalogTable().getOptions());
        }
    }

    /** A strict-mapping rejection is told by its wording: Flink's unsupported-option error lists the option's key too. */
    private ValidationException withCatalogRemedy(ValidationException failure, Map<String, String> tableOptions) {
        String option = SINK_STRICT_NUMERIC_MAPPING.key();
        boolean strictRejection = isStrict(tableOptions)
                && failure.getMessage() != null
                && failure.getMessage().contains("which '" + option + "' forbids");
        if (!strictRejection) {
            return failure;
        }
        String remedy = strictCatalog
                ? String.format("remove '%s' from the catalog, or override it for a statement with "
                        + "/*+ OPTIONS('%s' = 'false') */", option, option)
                : String.format("remove '%s' from the statement's OPTIONS hint", option);
        return new ValidationException(String.format(
                "%s A catalog table's Flink types are derived, so this one cannot be redeclared: %s.",
                failure.getMessage(), remedy), failure);
    }

    private static boolean isStrict(Map<String, String> options) {
        return Boolean.parseBoolean(options.get(SINK_STRICT_NUMERIC_MAPPING.key()));
    }
}
