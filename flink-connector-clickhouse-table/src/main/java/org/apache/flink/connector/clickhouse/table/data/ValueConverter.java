package org.apache.flink.connector.clickhouse.table.data;

import java.io.Serializable;

/**
 * Unwraps one non-null Flink-internal value ({@code StringData}, {@code DecimalData},
 * {@code TimestampData}, {@code ArrayData}, …) into the plain, TypeTags-legal Java value
 * the payload {@code Map} carries ({@code String}, {@code BigDecimal}, {@code UUID}, …).
 *
 * <p>Converters are chosen at planning time from the (Flink {@code LogicalType},
 * {@code ClickHouseColumn}) pair — the same Flink value unwraps to different Java classes
 * depending on the target column (e.g. {@code StringData} → {@code String} for a ClickHouse
 * {@code String} column but {@code java.util.UUID} for a {@code UUID} column).
 *
 * <p>Shipped to TaskManagers inside {@code RowDataDataMapper}; implementations must only
 * capture serializable, Flink-version-stable state (never a {@code ClickHouseColumn}).
 */
@FunctionalInterface
public interface ValueConverter extends Serializable {

    /** Converts a non-null Flink-internal value; never invoked with {@code null}. */
    Object convert(Object value);
}
