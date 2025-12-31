package org.apache.flink.connector.clickhouse.sink.convertor;

import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.utils.Serialize;
import org.apache.flink.connector.clickhouse.convertor.POJOConvertor;
import org.apache.flink.connector.clickhouse.sink.pojo.SimplePOJOWithDateTime;

import java.io.IOException;
import java.io.OutputStream;
import java.time.ZoneOffset;

public class SimplePOJOWithDateTimeConvertor extends POJOConvertor<SimplePOJOWithDateTime> {

    @Override
    public void instrument(OutputStream out, SimplePOJOWithDateTime input) throws IOException {
        Serialize.writeString(out, input.getId(), false, false, ClickHouseDataType.String, false, "id");
        Serialize.writeTimeDate64(out, input.getCreatedAt().atZone(ZoneOffset.UTC), false, false, ClickHouseDataType.DateTime64, false, "createdAt", 3);
        Serialize.writeInt32(out, input.getNumLogins(), false, false, ClickHouseDataType.Int32, false, "numLogins");
    }
}
