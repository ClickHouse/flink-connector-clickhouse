package com.clickhouse.utils;

import org.apache.flink.connector.clickhouse.exception.FlinkWriteException;
import org.apache.flink.connector.clickhouse.exception.RetriableException;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;

class UtilsTest {

    private static IOException ioException(String message) {
        return new IOException(message);
    }

    @Test void readTimedOutIsRetriable() {
        assertThrows(RetriableException.class,
                () -> Utils.handleException(ioException("Read timed out after 5000ms")));
    }

    @Test void writeTimedOutIsRetriable() {
        assertThrows(RetriableException.class,
                () -> Utils.handleException(ioException("Write timed out after 5000ms")));
    }

    @Test void insertFailedIsRetriable() {
        assertThrows(RetriableException.class,
                () -> Utils.handleException(ioException("Insert failed: connection dropped")));
    }

    @Test void insertFailedAsSuffixIsRetriable() {
        // "Insert failed" is a substring check (> -1), not a starts-with check
        assertThrows(RetriableException.class,
                () -> Utils.handleException(ioException("Batch Insert failed")));
    }

    @Test void connectionResetByPeerIsRetriable() {
        assertThrows(RetriableException.class,
                () -> Utils.handleException(ioException("Connection reset by peer")));
    }

    @Test void brokenPipeIsRetriable() {
        assertThrows(RetriableException.class,
                () -> Utils.handleException(ioException("Broken pipe")));
    }

    @Test void connectionTimedOutIsRetriable() {
        assertThrows(RetriableException.class,
                () -> Utils.handleException(ioException("Connection timed out")));
    }

    @Test void connectionTimedOutAsSuffixIsRetriable() {
        assertThrows(RetriableException.class,
                () -> Utils.handleException(ioException("java.net.SocketException: Connection timed out")));
    }

    @Test void unknownIoExceptionMessageThrowsFlinkWriteException() {
        assertThrows(FlinkWriteException.class,
                () -> Utils.handleException(ioException("Some unexpected IO error")));
    }

    @Test void wrappedInsertFailedIsRetriable() {
        RuntimeException wrapper = new RuntimeException(ioException("Insert failed: timeout"));
        assertThrows(RetriableException.class, () -> Utils.handleException(wrapper));
    }
}
