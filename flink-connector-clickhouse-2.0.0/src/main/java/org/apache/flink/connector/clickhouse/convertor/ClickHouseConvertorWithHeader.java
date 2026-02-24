/*
 * Copyright (c) 2026 Aviatrix, Inc.
 * 
 * All rights reserved.
 */

package org.apache.flink.connector.clickhouse.convertor;

import java.io.IOException;

import com.clickhouse.utils.writer.DataWriter;

public class ClickHouseConvertorWithHeader<InputT> extends ClickHouseConvertor<InputT> implements SupportsHeader {
    public ClickHouseConvertorWithHeader(Class<?> clazz) {
        super(clazz);
    }

    public <T extends POJOConvertor<InputT> & SupportsHeader> ClickHouseConvertorWithHeader(Class<?> clazz,
            T pojoConvertor) {
        super(clazz, pojoConvertor);
    }

    @Override
    public void header(DataWriter dataWriter) throws IOException {
        SupportsHeader convertor = (SupportsHeader) this.pojoConvertor;
        convertor.header(dataWriter);
    }
}
