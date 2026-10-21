package org.apache.flink.connector.clickhouse.token;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/** Receives the ClickHouse JWT on every TaskManager and publishes it to {@link ClickHouseCredentialsHolder}. */
public class ClickHouseDelegationTokenReceiver implements DelegationTokenReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseDelegationTokenReceiver.class);

    @Override
    public String serviceName() {
        return ClickHouseDelegationTokenProvider.SERVICE_NAME;
    }

    @Override
    public void init(Configuration configuration) {
        // Nothing to configure: the JWT carries everything the sink needs.
    }

    @Override
    public void onNewTokensObtained(byte[] tokens) {
        if (tokens.length == 0) {
            // The provider's "none yet" marker: the identity provider failed before the first token.
            LOG.debug("The JobManager has no ClickHouse JWT yet (identity provider unavailable); nothing to apply");
            return;
        }
        ClickHouseCredentials credentials = ClickHouseCredentialsCodec.decode(tokens);
        ClickHouseCredentialsHolder.set(credentials);
        LOG.info("Received ClickHouse JWT {} valid until {}", ClickHouseCredentials.redact(credentials.getJwt()),
                Instant.ofEpochMilli(credentials.getValidUntilMillis()));
    }
}
