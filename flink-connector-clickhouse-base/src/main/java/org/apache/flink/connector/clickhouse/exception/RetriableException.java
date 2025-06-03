package org.apache.flink.connector.clickhouse.exception;

public class RetriableException extends FlinkWriteException {
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
