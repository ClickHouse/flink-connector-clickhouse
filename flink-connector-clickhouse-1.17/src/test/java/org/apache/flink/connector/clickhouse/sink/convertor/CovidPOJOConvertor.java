package org.apache.flink.connector.clickhouse.sink.convertor;

import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.utils.Serialize;
import com.clickhouse.utils.writer.DataWriter;
import org.apache.flink.connector.clickhouse.convertor.POJOConvertor;
import org.apache.flink.connector.clickhouse.sink.pojo.CovidPOJO;

import java.io.IOException;
import java.io.OutputStream;

public class CovidPOJOConvertor extends POJOConvertor<CovidPOJO> {

    public CovidPOJOConvertor(boolean hasDefaults) {
        super(hasDefaults);
    }

    @Override
    public void instrument(DataWriter dataWriter, CovidPOJO input) throws IOException {
        dataWriter.writeDate(input.getLocalDate(), false, ClickHouseDataType.Date, false, "date");
        dataWriter.writeString(input.getLocation_key(), false,  ClickHouseDataType.String, false, "location_key");
        dataWriter.writeInt32(input.getNew_confirmed(), false,  ClickHouseDataType.Int32, false, "new_confirmed");
        dataWriter.writeInt32(input.getNew_deceased(), false,  ClickHouseDataType.Int32, false, "new_deceased");
        dataWriter.writeInt32(input.getNew_recovered(), false, ClickHouseDataType.Int32, false, "new_recovered");
        dataWriter.writeInt32(input.getNew_tested(), false,  ClickHouseDataType.Int32, false, "new_tested");
        dataWriter.writeInt32(input.getCumulative_confirmed(),  false, ClickHouseDataType.Int32, false, "cumulative_confirmed");
        dataWriter.writeInt32(input.getCumulative_deceased(),  false, ClickHouseDataType.Int32, false, "cumulative_deceased");
        dataWriter.writeInt32(input.getCumulative_recovered(), false, ClickHouseDataType.Int32, false, "cumulative_recovered");
        dataWriter.writeInt32(input.getCumulative_tested(),  false, ClickHouseDataType.Int32, false, "cumulative_tested");
    }
}
