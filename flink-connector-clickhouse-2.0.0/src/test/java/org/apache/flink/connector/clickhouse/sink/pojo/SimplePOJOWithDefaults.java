package org.apache.flink.connector.clickhouse.sink.pojo;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * For testing writing to a CH table with a default column.
 */
public class SimplePOJOWithDefaults {

    private final int id;

    private final LocalDateTime createdOn;

    public SimplePOJOWithDefaults(int index) {
        this.id = index;
        this.createdOn = index % 2 == 0 ? null : ZonedDateTime.of(LocalDateTime.now(), ZoneId.of("UTC")).toLocalDateTime();
    }

    public int getId() {
        return id;
    }

    public LocalDateTime getCreatedOn() {
        return createdOn;
    }
}
