package org.apache.flink.connector.clickhouse.table.schema;

import java.time.ZoneId;
import java.util.Objects;

/**
 * What the type matrix needs from the connector options: the zone {@code TIMESTAMP} wall clocks
 * are interpreted in, and whether a pair that would need a per-record check is rejected at planning
 * ({@code sink.strict-type-mapping}).
 */
public final class TypeMappingOptions {

    public final ZoneId sinkTimezone;
    public final boolean strict;

    public TypeMappingOptions(ZoneId sinkTimezone, boolean strict) {
        this.sinkTimezone = Objects.requireNonNull(sinkTimezone, "sinkTimezone");
        this.strict = strict;
    }
}
