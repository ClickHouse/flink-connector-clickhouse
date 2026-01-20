package org.apache.flink.connector.clickhouse.sink.pojo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

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
