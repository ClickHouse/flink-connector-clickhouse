package org.apache.flink.connector.test.embedded.clickhouse;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ClickHouseTestHelpers {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseTestHelpers.class);

    public static final String CLICKHOUSE_VERSION_DEFAULT = "24.3";
    public static final String CLICKHOUSE_PROXY_VERSION_DEFAULT = "23.8";
    public static final String CLICKHOUSE_DOCKER_IMAGE = String.format("clickhouse/clickhouse-server:%s", getClickhouseVersion());
    public static final String CLICKHOUSE_FOR_PROXY_DOCKER_IMAGE = String.format("clickhouse/clickhouse-server:%s", CLICKHOUSE_PROXY_VERSION_DEFAULT);

    public static final String HTTPS_PORT = "8443";
    public static final String DATABASE_DEFAULT = "default";
    public static final String USERNAME_DEFAULT = "default";

    private static final int CLOUD_TIMEOUT_VALUE = 900;
    private static final TimeUnit CLOUD_TIMEOUT_UNIT = TimeUnit.SECONDS;

    public static String getClickhouseVersion() {
        String clickHouseVersion = System.getenv("CLICKHOUSE_VERSION");
        if (clickHouseVersion == null) {
            clickHouseVersion = CLICKHOUSE_VERSION_DEFAULT;
        }
        return clickHouseVersion;
    }

    public static boolean isCloud() {
        String clickHouseVersion = System.getenv("CLICKHOUSE_VERSION");
        return clickHouseVersion != null && clickHouseVersion.equalsIgnoreCase("cloud");
    }

    public static Client getClient(boolean isCloud, String host, int port, boolean ssl, String username, String password) {
        return new Client.Builder().addEndpoint(Protocol.HTTP, host, port, ssl)
                                   .setUsername(username)
                                   .setPassword(password)
                                   .build();
    }

    public static boolean ping(boolean isCloud, String host, int port, boolean ssl, String username, String password) {
        Client client = getClient(isCloud(), host, port, ssl, username, password);
        return client.ping();
    }

}
