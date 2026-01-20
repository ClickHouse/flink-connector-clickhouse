package org.apache.flink.connector.clickhouse.sink.pojo;

public class SimplePOJOWithJSON {

    private long longPrimitive;

    private String jsonString;

    public SimplePOJOWithJSON(int index) {

        this.longPrimitive = index;
        this.jsonString = String.format("{\"index\" : \"%s\", \"bar\" : \"foo\" }", index);
    }


    public long getLongPrimitive() {
        return longPrimitive;
    }

    public String getJsonString() { return jsonString; }

}
