package org.apache.flink.connector.clickhouse.table;

import com.clickhouse.data.ClickHouseFormat;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.connector.clickhouse.table.data.RowDataDataMapper;
import org.apache.flink.connector.clickhouse.table.schema.ServerComputedColumns;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.DATABASE;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.SINK_BUFFER_FLUSH_MAX_BYTES;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.SINK_MAX_BUFFERED_REQUESTS;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.SINK_MAX_IN_FLIGHT_REQUESTS;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.SINK_RECORD_MAX_BYTES;
import static org.apache.flink.connector.clickhouse.table.ClickHouseConnectorOptions.TABLE;

/**
 * Wraps the existing {@link ClickHouseAsyncSink} behind Flink's {@code DynamicTableSink}
 * contract with an insert-only changelog; the planner rejects update-producing queries.
 * Upsert is issue #148.
 */
public class ClickHouseDynamicTableSink implements DynamicTableSink {

    private final ClickHouseClientConfig clientConfig;
    private final RowDataDataMapper mapper;
    /** The factory-validated table options, read here so a batching option is named in one place. */
    private final ReadableConfig options;
    /** Names the {@code INSERT} column list's indices refer to; see {@link TargetColumns}. */
    private final ResolvedSchema schema;
    /** Declared but unwritable; only the statement's column list decides whether that matters. */
    private final ServerComputedColumns serverComputed;

    public ClickHouseDynamicTableSink(ClickHouseClientConfig clientConfig, RowDataDataMapper mapper,
                                      ReadableConfig options, ResolvedSchema schema,
                                      ServerComputedColumns serverComputed) {
        this.clientConfig = Objects.requireNonNull(clientConfig, "clientConfig");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.options = Objects.requireNonNull(options, "options");
        this.schema = Objects.requireNonNull(schema, "schema");
        this.serverComputed = Objects.requireNonNull(serverComputed, "serverComputed");
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // Appending retractions would corrupt the table — #148's upsert lands here.
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        // A partial insert must leave the columns it omitted to the server, not write the
        // planner's padding nulls over their DEFAULTs.
        Optional<Set<String>> targeted = TargetColumns.resolve(context, schema);
        // A MATERIALIZED/ALIAS/EPHEMERAL column is only an error once the statement writes it.
        serverComputed.checkNotWritten(targeted, TargetColumns.isSupported());
        RowDataDataMapper effectiveMapper = targeted.map(mapper::project).orElse(mapper);
        return SinkV2Provider.of(buildSink(effectiveMapper),
                options.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null));
    }

    private ClickHouseAsyncSink<RowData> buildSink(RowDataDataMapper effectiveMapper) {
        return ClickHouseAsyncSink.<RowData>builder()
                .setElementConverter(new ClickHouseConvertor<>(RowData.class, effectiveMapper))
                .setClickHouseFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                .setMaxBatchSize(options.get(SINK_BUFFER_FLUSH_MAX_ROWS))
                .setMaxBatchSizeInBytes(options.get(SINK_BUFFER_FLUSH_MAX_BYTES).getBytes())
                .setMaxTimeInBufferMS(options.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis())
                .setMaxInFlightRequests(options.get(SINK_MAX_IN_FLIGHT_REQUESTS))
                .setMaxBufferedRequests(options.get(SINK_MAX_BUFFERED_REQUESTS))
                .setMaxRecordSizeInBytes(options.get(SINK_RECORD_MAX_BYTES).getBytes())
                .setClickHouseClientConfig(clientConfig)
                // The factory's DESCRIBE proved connectivity; a planner hook must not go back to the network.
                .setVerifyConnectivity(false)
                .build();
    }

    @Override
    public DynamicTableSink copy() {
        // The config is mutable (setters, cached client) and must not be shared; the rest is read-only.
        return new ClickHouseDynamicTableSink(clientConfig.copy(), mapper, options, schema,
                serverComputed);
    }

    @Override
    public String asSummaryString() {
        return "ClickHouse[" + options.get(DATABASE) + "." + options.get(TABLE) + "]";
    }
}
