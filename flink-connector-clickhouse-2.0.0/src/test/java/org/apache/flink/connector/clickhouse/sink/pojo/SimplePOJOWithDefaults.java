package org.apache.flink.connector.clickhouse.sink.pojo;

import java.time.LocalDateTime;

/**
 * For testing most data types.
 */
public class SimplePOJOWithDefaults {

    private String id;

    private LocalDateTime createdOn;

    public SimplePOJOWithDefaults(int index) {
        this.id = "str" + index;
        this.createdOn = (index % 2 == 0 ? null : LocalDateTime.now());
    }

    public  String getId() {
        return id;
    }

    public LocalDateTime getCreatedOn() {
        return createdOn;
    }
}
