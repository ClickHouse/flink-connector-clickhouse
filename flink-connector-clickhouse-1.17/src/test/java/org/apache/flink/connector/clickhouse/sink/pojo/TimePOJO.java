package org.apache.flink.connector.clickhouse.sink.pojo;

public class TimePOJO implements java.io.Serializable {
    public String id;
    public long elapsedNanos;
    public DataType dataType;
    public int precision;

    public TimePOJO() {
    }

    public TimePOJO(String id, long elapsedNanos, DataType dataType, int precision) {
        this.id = id;
        this.elapsedNanos = elapsedNanos;
        this.dataType = dataType;
        this.precision = precision;
    }

    public enum DataType {
        TIME,
        TIME64
    }

    // Time needs enable_time_time64_type before ClickHouse 25.12.
    public static String createTableSql(String database, String tableName, String timeType) {
        return "CREATE TABLE `" + database + "`.`" + tableName + "` (" +
                "id String," +
                String.format("elapsed %s", timeType) +
                ") " +
                "ENGINE = MergeTree " +
                "ORDER BY (id) " +
                "SETTINGS enable_time_time64_type = 1";
    }
}
