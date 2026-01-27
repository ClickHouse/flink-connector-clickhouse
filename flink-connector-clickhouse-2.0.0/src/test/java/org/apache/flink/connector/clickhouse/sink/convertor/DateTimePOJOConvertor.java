package org.apache.flink.connector.clickhouse.sink.convertor;

import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.utils.Serialize;
import org.apache.flink.connector.clickhouse.convertor.POJOConvertor;
import org.apache.flink.connector.clickhouse.sink.pojo.DateTimePOJO;

import java.io.IOException;
import java.io.OutputStream;
import java.time.ZoneOffset;

public class DateTimePOJOConvertor extends POJOConvertor<DateTimePOJO> {

    @Override
    public void instrument(OutputStream out, DateTimePOJO input) throws IOException {
        Serialize.writeString(out, input.id, false, false, ClickHouseDataType.String, false, "id");
        if (input.dataType.equals(DateTimePOJO.DataType.DATETIME64)) {
            Serialize.writeTimeDate64(out, input.createdAt.atZone(ZoneOffset.UTC), false, false, ClickHouseDataType.DateTime64, false, "createdAt", input.precision);
        } else {
            Serialize.writeTimeDate(out, input.createdAt.atZone(ZoneOffset.UTC), false, false, ClickHouseDataType.DateTime, false, "createdAt");
        }
        Serialize.writeInt32(out, input.numLogins, false, false, ClickHouseDataType.Int32, false, "numLogins");
    }
}
