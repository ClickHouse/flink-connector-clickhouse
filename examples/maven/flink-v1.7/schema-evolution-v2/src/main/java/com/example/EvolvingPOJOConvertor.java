package com.example;

import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.utils.writer.DataWriter;
import org.apache.flink.connector.clickhouse.convertor.POJOConvertor;

import java.io.IOException;

/**
 * V2 convertor: writes 3 columns matching the post-ALTER table schema
 * (id, name, ts). For records that came from V1 state via schema migration,
 * {@code ts} will be 0 (Java default) which lines up with the table's
 * {@code DEFAULT 0}.
 */
public class EvolvingPOJOConvertor extends POJOConvertor<EvolvingPOJO> {

    public EvolvingPOJOConvertor(boolean schemaHasDefaults) {
        super(schemaHasDefaults);
    }

    @Override
    public void instrument(DataWriter dataWriter, EvolvingPOJO input) throws IOException {
        dataWriter.writeInt32(input.getId(), false, ClickHouseDataType.Int32, false, "id");
        dataWriter.writeString(input.getName(), false, ClickHouseDataType.String, false, "name");
        dataWriter.writeInt64(input.getTs(), false, ClickHouseDataType.Int64, false, "ts");
    }
}
