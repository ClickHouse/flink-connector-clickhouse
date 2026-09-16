package org.apache.flink.connector.clickhouse.table.schema;

import java.time.ZoneId;
import java.util.Objects;

/**
 * What the type matrix needs from the connector options: the zone {@code TIMESTAMP} wall clocks
 * are interpreted in, and whether a numeric pair that would need a per-record range check is rejected
 * at planning ({@code sink.strict-numeric-mapping}).
 */
public final class TypeMappingOptions {

    public final ZoneId sinkTimezone;
    public final boolean strictNumeric;

    public TypeMappingOptions(ZoneId sinkTimezone, boolean strictNumeric) {
        this.sinkTimezone = Objects.requireNonNull(sinkTimezone, "sinkTimezone");
        this.strictNumeric = strictNumeric;
    }
}
