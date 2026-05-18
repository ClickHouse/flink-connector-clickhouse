package org.apache.flink.connector.clickhouse.convertor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * User-implemented adapter from arbitrary input records (POJO, Protobuf, Avro, …)
 * to a {@code Map<String, Object>} payload + a declarative list of ClickHouse columns.
 *
 * <p>See design spec §5 in docs/superpowers/specs/2026-05-16-map-based-payload-design.md.
 *
 * <p>Allowed Map value types:
 * {@code null}, {@code Boolean}, {@code Byte}, {@code Short}, {@code Integer},
 * {@code Long}, {@code Float}, {@code Double}, {@code BigInteger}, {@code BigDecimal},
 * {@code String}, {@code byte[]}, {@code UUID}, {@code LocalDate},
 * {@code LocalDateTime}, {@code ZonedDateTime}, {@code List<allowed>},
 * {@code Map<String, allowed>}, {@code Object[]} (tuples).
 * Anything else fails at checkpoint serialization time.
 */
public abstract class DataMapper<InputT> implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Populate the map from the input record. Only fields put here are checkpointed.
     */
    public abstract void toMap(InputT input, Map<String, Object> map);

    /**
     * Declare the ClickHouse columns. Called once at sink open(); the returned list
     * is cached. Order is the order columns are written within each row.
     */
    public abstract List<ColumnBinding> bindings();
}
