package org.apache.flink.connector.clickhouse.sink.pojo;

public class SimplePOJOWithJSON implements java.io.Serializable {

    private final long longPrimitive;

    private final String jsonString;

    public SimplePOJOWithJSON(int index) {

        this.longPrimitive = index;
        this.jsonString = String.format("{\"index\" : \"%s\", \"bar\" : \"foo\" }", index);
    }


    public long getLongPrimitive() {
        return longPrimitive;
    }

    public String getJsonString() {
        return jsonString;
    }

    public static String createTableSql(String database, String tableName) {
        return "CREATE TABLE `" + database + "`.`" + tableName + "` (" +
                "longPrimitive Int64," +
                "jsonPayload JSON," +
                ") " +
                "ENGINE = MergeTree " +
                "ORDER BY (longPrimitive); ";
    }
}
