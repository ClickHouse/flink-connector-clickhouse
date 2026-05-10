package com.example;

import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.utils.writer.DataWriter;
import org.apache.flink.connector.clickhouse.convertor.POJOConvertor;

import java.io.IOException;

public class EvolvingPOJOConvertor extends POJOConvertor<EvolvingPOJO> {

    public EvolvingPOJOConvertor(boolean schemaHasDefaults) {
        super(schemaHasDefaults);
    }

    @Override
    public void instrument(DataWriter dataWriter, EvolvingPOJO input) throws IOException {
        dataWriter.writeInt32(input.getId(), false, ClickHouseDataType.Int32, false, "id");
        dataWriter.writeString(input.getName(), false, ClickHouseDataType.String, false, "name");
    }
}
