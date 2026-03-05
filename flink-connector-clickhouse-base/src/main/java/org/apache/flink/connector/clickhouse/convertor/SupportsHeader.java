/*
 * Copyright (c) 2026 Aviatrix, Inc.
 * 
 * All rights reserved.
 */

package org.apache.flink.connector.clickhouse.convertor;

import java.io.IOException;

import com.clickhouse.utils.writer.DataWriter;

public interface SupportsHeader {
    void header(DataWriter dataWriter) throws IOException;
}
