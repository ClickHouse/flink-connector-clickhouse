package org.apache.flink.connector.clickhouse.sink.convertor;

import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.utils.Serialize;
import com.clickhouse.utils.writer.DataWriter;
import org.apache.flink.connector.clickhouse.convertor.POJOConvertor;
import org.apache.flink.connector.clickhouse.sink.pojo.SimplePOJOWithDateTime;

import java.io.IOException;
import java.io.OutputStream;
import java.time.ZoneOffset;

public class SimplePOJOWithDateTimeConvertor extends POJOConvertor<SimplePOJOWithDateTime> {

    public SimplePOJOWithDateTimeConvertor(boolean hasDefaults) {
        super(hasDefaults);
    }

    @Override
    public void instrument(DataWriter dataWriter, SimplePOJOWithDateTime input) throws IOException {
        dataWriter.writeString(input.getId(), false, ClickHouseDataType.String, false, "id");
        dataWriter.writeTimeDate64(input.getCreatedAt().atZone(ZoneOffset.UTC), false, ClickHouseDataType.DateTime64, false, "createdAt", 3);
        dataWriter.writeInt32(input.getNumLogins(), false, ClickHouseDataType.Int32, false, "numLogins");
    }
}
