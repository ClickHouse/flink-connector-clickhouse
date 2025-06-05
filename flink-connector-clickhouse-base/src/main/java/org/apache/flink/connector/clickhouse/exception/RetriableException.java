package org.apache.flink.connector.clickhouse.exception;

public class RetriableException extends FlinkWriteException {
    private static final long serialVersionUID = 1L;

    public RetriableException(String message) {
        super(message);
    }
    public RetriableException(String message, Throwable cause) {
        super(message, cause);
    }
    public RetriableException(Throwable cause) {
        super(cause);
    }
}
