package org.apache.flink.connector.clickhouse.sink.pojo;

import java.time.Instant;


/**
 * For testing the DateTime data type.
 * <p>
 * This class conforms to Flink's POJO definition - see: <a href="https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/fault-tolerance/serialization/types_serialization/#pojos">...</a>. This ensures that Flink does not use kryo to serialize this class, which can lead to unexpected runtime exceptions in certain setups.
 */
public class DateTimePOJO {
    public String id;
    public Instant createdAt;
    public DataType dataType;
    public int precision;
    public int numLogins;

    public DateTimePOJO() {
    }

    public DateTimePOJO(String id, Instant createdAt, DataType dataType, int precision, int numLogins) {
        this.id = id;
        this.createdAt = createdAt;
        this.dataType = dataType;
        this.precision = precision;
        this.numLogins = numLogins;
    }

    public enum DataType {
        DATETIME,
        DATETIME64
    }
}
