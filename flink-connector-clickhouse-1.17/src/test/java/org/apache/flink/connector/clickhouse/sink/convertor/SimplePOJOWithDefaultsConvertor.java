package org.apache.flink.connector.clickhouse.sink.convertor;

import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.utils.writer.DataWriter;
import org.apache.flink.connector.clickhouse.convertor.POJOConvertor;
import org.apache.flink.connector.clickhouse.sink.pojo.SimplePOJOWithDefaults;

import java.io.IOException;

public class SimplePOJOWithDefaultsConvertor extends POJOConvertor<SimplePOJOWithDefaults> {

    public SimplePOJOWithDefaultsConvertor(boolean schemaHasDefaults) {
        super(schemaHasDefaults);
    }

    @Override
    public void instrument(DataWriter dataWriter, SimplePOJOWithDefaults input) throws IOException {
       dataWriter.writeString(input.getId(), false, ClickHouseDataType.String, false, "id");
       dataWriter.writeTimeDate64(input.getCreatedOn(), false, ClickHouseDataType.DateTime64, true, "createdOn", 1);
    }

}
