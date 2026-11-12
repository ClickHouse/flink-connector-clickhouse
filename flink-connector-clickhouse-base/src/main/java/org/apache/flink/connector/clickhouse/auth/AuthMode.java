package org.apache.flink.connector.clickhouse.auth;

/** How the sink authenticates to ClickHouse. */
public enum AuthMode {
    /** Username/password configured in job code. */
    PASSWORD,
    /** Short-lived ClickHouse Cloud JWT from Flink's delegation token framework (flink-connector-clickhouse-token in lib/). */
    DELEGATION_TOKEN
}
