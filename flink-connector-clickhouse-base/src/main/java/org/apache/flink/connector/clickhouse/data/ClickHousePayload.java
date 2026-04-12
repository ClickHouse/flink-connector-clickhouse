package org.apache.flink.connector.clickhouse.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class ClickHousePayload implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHousePayload.class);
    private static final long serialVersionUID = 2L;

    private int attemptCount = 1;
    private byte[] payload;
    private Serializable originalInput;

    public ClickHousePayload(byte[] payload) {
        this.payload = payload;
    }

    public ClickHousePayload(byte[] payload, Serializable originalInput) {
        this.payload = payload;
        this.originalInput = originalInput;
    }

    public ClickHousePayload(Serializable originalInput) {
        this.payload = null;
        this.originalInput = originalInput;

    }

    public byte[] getPayload() { return payload; }
    public void setPayload(byte[] payload) { this.payload = payload; }
    public int getPayloadLength() {
        if (payload == null) return -1;
        return payload.length;
    }
    public Serializable getOriginalInput() { return originalInput; }
    public boolean needsRehydration() { return payload == null && originalInput != null; }
    public int getAttemptCount() { return attemptCount; }
    public void incrementAttempts() { attemptCount++; }
}
