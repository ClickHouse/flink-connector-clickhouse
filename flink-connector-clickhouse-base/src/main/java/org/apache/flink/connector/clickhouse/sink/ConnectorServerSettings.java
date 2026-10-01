package org.apache.flink.connector.clickhouse.sink;

/**
 * Server settings the connector sends itself, shared by ClickHouseAsyncWriter (which pins them per
 * insert) and the SQL connector's passthrough validation (which rejects a user copy, since
 * client-v2 lets the operation's settings win and would discard it silently).
 */
public final class ConnectorServerSettings {

    public static final String INPUT_FORMAT_NULL_AS_DEFAULT = "input_format_null_as_default";
    public static final String INPUT_FORMAT_DEFAULTS_FOR_OMITTED_FIELDS =
            "input_format_defaults_for_omitted_fields";

    private ConnectorServerSettings() {}
}
