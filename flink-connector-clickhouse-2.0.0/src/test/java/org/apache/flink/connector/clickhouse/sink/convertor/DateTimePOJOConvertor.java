package org.apache.flink.connector.clickhouse.sink.convertor;

import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.utils.Serialize;
import com.clickhouse.utils.writer.DataWriter;
import org.apache.flink.connector.clickhouse.convertor.POJOConvertor;
import org.apache.flink.connector.clickhouse.sink.pojo.DateTimePOJO;

import java.io.IOException;
import java.io.OutputStream;
import java.time.ZoneOffset;

public class DateTimePOJOConvertor extends POJOConvertor<DateTimePOJO> {

    public DateTimePOJOConvertor(boolean hasDefaults) {
        super(hasDefaults);
    }

    @Override
    public void instrument(DataWriter dataWriter, DateTimePOJO input) throws IOException {
        dataWriter.writeString(input.id, false, ClickHouseDataType.String, false, "id");
        if (input.dataType.equals(DateTimePOJO.DataType.DATETIME64)) {
            dataWriter.writeTimeDate64(input.createdAt.atZone(ZoneOffset.UTC),false, ClickHouseDataType.DateTime64, false, "createdAt", input.precision);
        } else {
            dataWriter.writeTimeDate(input.createdAt.atZone(ZoneOffset.UTC), false, ClickHouseDataType.DateTime, false, "createdAt");
        }
        dataWriter.writeInt32(input.numLogins, false, ClickHouseDataType.Int32, false, "numLogins");
    }
}
