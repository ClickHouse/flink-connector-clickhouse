package org.apache.flink.connector.clickhouse.exception;

public class FlinkWriteException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public FlinkWriteException(Throwable cause) {

    }
    public FlinkWriteException(String message, Throwable cause) {
        super(message, cause);
    }
    public FlinkWriteException(String message) {
        super(message);
    }
}
