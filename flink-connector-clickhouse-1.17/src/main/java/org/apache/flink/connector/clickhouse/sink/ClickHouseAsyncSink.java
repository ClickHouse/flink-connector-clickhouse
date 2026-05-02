package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.data.ClickHouseFormat;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

public class ClickHouseAsyncSink<InputT>
        extends AsyncSinkBase<InputT, ClickHousePayload<InputT>> {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseAsyncSink.class);

    protected final ClickHouseClientConfig clickHouseClientConfig;
    protected final ClickHouseFormat clickHouseFormat;
    private final TypeInformation<InputT> inputTypeInfo;

    /** Package-private — construct via {@link #builder()}. */
    ClickHouseAsyncSink(
            ElementConverter<InputT, ClickHousePayload<InputT>> converter,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInByte,
            ClickHouseClientConfig clickHouseClientConfig,
            ClickHouseFormat clickHouseFormat,
            TypeInformation<InputT> inputTypeInfo) {
        super(converter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInByte);
        this.clickHouseClientConfig =
                Objects.requireNonNull(clickHouseClientConfig, "ClickHouse config cannot be null");
        this.clickHouseFormat = clickHouseFormat;
        this.inputTypeInfo =
                Objects.requireNonNull(inputTypeInfo, "inputTypeInfo cannot be null");
    }

    public static <InputT> ClickHouseAsyncSinkBuilder<InputT> builder() {
        return new ClickHouseAsyncSinkBuilder<>();
    }

    public ClickHouseFormat getClickHouseFormat() { return this.clickHouseFormat; }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<ClickHousePayload<InputT>>> createWriter(
            InitContext context) throws IOException {
        return restoreWriter(context, Collections.emptyList());
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<ClickHousePayload<InputT>>> restoreWriter(
            Sink.InitContext context,
            Collection<BufferedRequestState<ClickHousePayload<InputT>>> collection)
            throws IOException {
        return new ClickHouseAsyncWriter<>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                clickHouseClientConfig,
                clickHouseFormat,
                collection);
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<ClickHousePayload<InputT>>>
            getWriterStateSerializer() {
        TypeSerializer<InputT> inputSerializer =
                inputTypeInfo.createSerializer(new ExecutionConfig());
        return new ClickHouseAsyncSinkSerializer<>(inputSerializer);
    }
}
