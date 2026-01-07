package org.apache.flink.connector.clickhouse.sink.pojo;

import java.time.Instant;

public class SimplePOJOWithDateTime {
    private String id;
    private Instant createdAt;
    private int numLogins;

    public SimplePOJOWithDateTime(String id, Instant createdAt, int numLogins) {
        this.id = id;
        this.createdAt = createdAt;
        this.numLogins = numLogins;
    }

    public String getId() { return id; }
    public Instant getCreatedAt() { return createdAt; }
    public int getNumLogins() { return numLogins; }
}
