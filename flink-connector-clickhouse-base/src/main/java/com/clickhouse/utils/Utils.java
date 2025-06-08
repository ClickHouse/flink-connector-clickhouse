package com.clickhouse.utils;

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

        //Let's check if we have a ServerException to reference the error code
        //https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp
        Exception rootCause = Utils.getRootCause(e, true);
        if (rootCause instanceof ServerException) {
            ServerException clickHouseServerException = (ServerException) rootCause;
            LOG.warn("ClickHouse Server Exception Code: {}", clickHouseServerException.getCode());
            switch (clickHouseServerException.getCode()) {
                case 3: // UNEXPECTED_END_OF_FILE
                case 107: // FILE_DOESNT_EXIST
                case 159: // TIMEOUT_EXCEEDED
                case 164: // READONLY
                case 202: // TOO_MANY_SIMULTANEOUS_QUERIES
                case 203: // NO_FREE_CONNECTION
                case 209: // SOCKET_TIMEOUT
                case 210: // NETWORK_ERROR
                case 241: // MEMORY_LIMIT_EXCEEDED
                case 242: // TABLE_IS_READ_ONLY
                case 252: // TOO_MANY_PARTS
                case 285: // TOO_FEW_LIVE_REPLICAS
                case 319: // UNKNOWN_STATUS_OF_INSERT
                case 425: // SYSTEM_ERROR
                case 999: // KEEPER_EXCEPTION
                    throw new RetriableException(e);
                default:
                    LOG.error("Error code [{}] wasn't in the acceptable list.", clickHouseServerException.getCode());
                    break;
            }
        }

        //Otherwise use Root-Cause Exception Checking
        if (rootCause instanceof SocketTimeoutException) {
            LOG.warn("SocketTimeoutException thrown, wrapping exception: {}", e.getLocalizedMessage());
            throw new RetriableException(e);
        } else if (rootCause instanceof UnknownHostException) {
            LOG.warn("UnknownHostException thrown, wrapping exception: {}", e.getLocalizedMessage());
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
