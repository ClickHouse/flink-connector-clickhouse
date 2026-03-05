/*
 * Copyright (c) 2026 Aviatrix, Inc.
 * 
 * All rights reserved.
 */

package org.apache.flink.connector.clickhouse.sink.convertor;

import java.io.IOException;

import org.apache.flink.connector.clickhouse.convertor.SupportsHeader;

import com.clickhouse.utils.writer.DataWriter;

public class CovidPOJOWithHeaderConvertor extends CovidPOJOConvertor implements SupportsHeader {
    public CovidPOJOWithHeaderConvertor() {
        super(false); // RowBinaryWithNames is incompatible with defaults
    }

    @Override
    public void header(DataWriter dataWriter) throws IOException {  
        String[] columns = {
            "date"                ,
            "location_key"        ,
            "new_confirmed"       ,
            "new_deceased"        ,
            "new_recovered"       ,
            "new_tested"          ,
            "cumulative_confirmed",
            "cumulative_deceased" ,
            "cumulative_recovered",
            "cumulative_tested"   
        };

        dataWriter.writeVarInt(columns.length);
        for (String col : columns) {
            dataWriter.writeRawString(col);
        }
    }
}
