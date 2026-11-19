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

    ClickHouseCatalogSinkFactory(DynamicTableSinkFactory sinkFactory) {
        this.sinkFactory = Objects.requireNonNull(sinkFactory, "sinkFactory");
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

    /** Only a strict-mapping rejection names the option while it is on. */
    private static ValidationException withCatalogRemedy(ValidationException failure,
                                                         Map<String, String> tableOptions) {
        String option = SINK_STRICT_NUMERIC_MAPPING.key();
        boolean strictRejection = Boolean.parseBoolean(tableOptions.get(option))
                && failure.getMessage() != null && failure.getMessage().contains(option);
        if (!strictRejection) {
            return failure;
        }
        return new ValidationException(String.format(
                "%s A catalog table's Flink types are derived, so this one cannot be redeclared: "
                + "remove '%s' from the catalog, or override it for a statement with "
                + "/*+ OPTIONS('%s' = 'false') */.",
                failure.getMessage(), option, option), failure);
    }
}
