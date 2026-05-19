package org.apache.flink.connector.clickhouse.exception;

public class DataCorruptionException extends FlinkWriteException {
    private static final long serialVersionUID = 1L;

    public DataCorruptionException(String message) {
        super(message);
    }
    public DataCorruptionException(String message, Throwable cause) {
        super(message, cause);
    }
    public DataCorruptionException(Throwable cause) {
        super(cause);
    }
}
