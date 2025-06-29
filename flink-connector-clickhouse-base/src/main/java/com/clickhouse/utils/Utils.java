package com.clickhouse.utils;

import com.clickhouse.client.api.ConnectionInitiationException;
import com.clickhouse.client.api.ServerException;
import org.apache.flink.connector.clickhouse.exception.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

public class Utils {

    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
    private static final String CLICKHOUSE_CLIENT_ERROR_READ_TIMEOUT_MSG = "Read timed out after";
    private static final String CLICKHOUSE_CLIENT_ERROR_WRITE_TIMEOUT_MSG = "Write timed out after";

    /**
     * This will drill down to the first ServerException in the exception chain
     *
     * @param e Exception to drill down
     * @return ServerException or null if none found
     */
    public static Exception getRootCause(Throwable e, Boolean prioritizeServerException) {
        if (e == null)
            return null;

        Throwable runningException = e;//We have to use Throwable because of the getCause() signature
        while (runningException.getCause() != null &&
                (!prioritizeServerException || !(runningException instanceof ServerException))) {
            LOG.trace("Found exception: {}", runningException.getLocalizedMessage());
            runningException = runningException.getCause();
        }

        return runningException instanceof Exception ? (Exception) runningException : null;
    }

    /**
     * This method checks to see if we should retry, otherwise it just throws the exception again
     *
     * @param e Exception to check
     */

    public static void handleException(Throwable e) {
        LOG.warn("Deciding how to handle exception: {}", e.getLocalizedMessage());
        Exception rootCause = Utils.getRootCause(e, true);
        if (rootCause instanceof ServerException) {
            ServerException serverException = (ServerException) rootCause;
            LOG.warn("ClickHouse Server Exception Code: {} isRetryable: {}", serverException.getCode(), serverException.isRetryable());
            System.out.println("ClickHouse Server Exception Code: " + serverException.getCode());
            if (serverException.isRetryable()) {
                throw new RetriableException(e);
            }
        } else if (rootCause instanceof ConnectionInitiationException) {
            LOG.warn("ClickHouse Connection Initiation Exception: {}", rootCause.getLocalizedMessage());
            throw new RetriableException(e);
        } else if (rootCause instanceof SocketTimeoutException) {
            LOG.warn("SocketTimeoutException thrown, wrapping exception: {}", e.getLocalizedMessage());
            throw new RetriableException(e);
        } else if (rootCause instanceof IOException) {
            final String msg = rootCause.getMessage();
            if (msg.indexOf(CLICKHOUSE_CLIENT_ERROR_READ_TIMEOUT_MSG) == 0 || msg.indexOf(CLICKHOUSE_CLIENT_ERROR_WRITE_TIMEOUT_MSG) == 0) {
                LOG.warn("IOException thrown, wrapping exception: {}", e.getLocalizedMessage());
                throw new RetriableException(e);
            }
        }
    }
}
