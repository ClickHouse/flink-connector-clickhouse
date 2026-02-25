/*
 * Copyright (c) 2026 Aviatrix Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
