package org.apache.flink.connector.clickhouse.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class ClickHousePayload implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHousePayload.class);
    private static final long serialVersionUID = 1L;

    private final byte[] payload;
    public ClickHousePayload(byte[] payload) {
        this.payload = payload;
    }
    public byte[] getPayload() { return payload; }
    public int getPayloadLength() { return payload.length; }
}
