package org.apache.flink.connector.clickhouse.table;

import com.clickhouse.data.ClickHouseFormat;

import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.connector.clickhouse.table.data.RowDataDataMapper;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;

import java.util.Objects;

/**
 * The SQL/Table API sink: wraps the existing {@link ClickHouseAsyncSink} behind Flink's
 * {@code DynamicTableSink} contract with an insert-only changelog
 * (docs/table-api/dld-ClickHouseDynamicTableSink.md). Update-producing queries are
 * rejected by the planner; the documented workaround is ReplacingMergeTree/
 * AggregatingMergeTree + FINAL/argMax, or issue #148 (upsert) once it lands.
 */
public class ClickHouseDynamicTableSink implements DynamicTableSink {

    private final ClickHouseClientConfig clientConfig;
    private final RowDataDataMapper mapper;
    private final int maxBatchSize;
    private final long maxBatchSizeInBytes;
    private final long maxTimeInBufferMs;
    private final int maxInFlightRequests;
    private final int maxBufferedRequests;
    private final long maxRecordSizeInBytes;
    /** Null means "use the query's parallelism". */
    private final Integer parallelism;
    private final String summaryName;

    public ClickHouseDynamicTableSink(ClickHouseClientConfig clientConfig,
                                      RowDataDataMapper mapper,
                                      int maxBatchSize,
                                      long maxBatchSizeInBytes,
                                      long maxTimeInBufferMs,
                                      int maxInFlightRequests,
                                      int maxBufferedRequests,
                                      long maxRecordSizeInBytes,
                                      Integer parallelism,
                                      String summaryName) {
        this.clientConfig = Objects.requireNonNull(clientConfig, "clientConfig");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.maxBatchSize = maxBatchSize;
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.maxTimeInBufferMs = maxTimeInBufferMs;
        this.maxInFlightRequests = maxInFlightRequests;
        this.maxBufferedRequests = maxBufferedRequests;
        this.maxRecordSizeInBytes = maxRecordSizeInBytes;
        this.parallelism = parallelism;
        this.summaryName = Objects.requireNonNull(summaryName, "summaryName");
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // Appending retractions would corrupt the table — #148's upsert lands here
        // as sign/version columns injected as extra ColumnBindings, no rewrite.
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        ClickHouseAsyncSink<RowData> sink = buildSink();
        return parallelism == null
                ? SinkV2Provider.of(sink)
                : SinkV2Provider.of(sink, parallelism);
    }

    private ClickHouseAsyncSink<RowData> buildSink() {
        return ClickHouseAsyncSink.<RowData>builder()
                .setElementConverter(new ClickHouseConvertor<>(RowData.class, mapper))
                .setClickHouseFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                .setMaxBatchSize(maxBatchSize)
                .setMaxBatchSizeInBytes(maxBatchSizeInBytes)
                .setMaxTimeInBufferMS(maxTimeInBufferMs)
                .setMaxInFlightRequests(maxInFlightRequests)
                .setMaxBufferedRequests(maxBufferedRequests)
                .setMaxRecordSizeInBytes(maxRecordSizeInBytes)
                .setClickHouseClientConfig(clientConfig)
                .build();
    }

    @Override
    public DynamicTableSink copy() {
        // All state is planning-time immutable, so sharing the references is safe.
        return new ClickHouseDynamicTableSink(clientConfig, mapper, maxBatchSize,
                maxBatchSizeInBytes, maxTimeInBufferMs, maxInFlightRequests,
                maxBufferedRequests, maxRecordSizeInBytes, parallelism, summaryName);
    }

    @Override
    public String asSummaryString() {
        return "ClickHouse[" + summaryName + "]";
    }
}
