package org.apache.flink.connector.clickhouse.sink;


import com.clickhouse.data.ClickHouseFormat;
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

public class ClickHouseAsyncSink<InputT> extends AsyncSinkBase<InputT, ClickHousePayload> {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseAsyncSink.class);

    protected ClickHouseClientConfig clickHouseClientConfig;
    protected ClickHouseFormat clickHouseFormat = null;

    public ClickHouseAsyncSink(
        ElementConverter<InputT, ClickHousePayload> converter,
        int maxBatchSize,
        int maxInFlightRequests,
        int maxBufferedRequests,
        long maxBatchSizeInBytes,
        long maxTimeInBufferMS,
        long maxRecordSizeInByte,
        ClickHouseClientConfig clickHouseClientConfig
    ) {
        super(converter,
              maxBatchSize,
              maxInFlightRequests,
              maxBufferedRequests,
              maxBatchSizeInBytes,
              maxTimeInBufferMS,
              maxRecordSizeInByte);

        this.clickHouseClientConfig = Objects.requireNonNull(clickHouseClientConfig, "ClickHouse config cannot be null");;
    }

    public void setClickHouseFormat(ClickHouseFormat clickHouseFormat) {
        this.clickHouseFormat = clickHouseFormat;
    }

    public ClickHouseFormat getClickHouseFormat() { return this.clickHouseFormat; }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<ClickHousePayload>> createWriter(InitContext context) throws IOException {
        return restoreWriter(context, Collections.emptyList());
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<ClickHousePayload>> restoreWriter(Sink.InitContext context, Collection<BufferedRequestState<ClickHousePayload>> collection) throws IOException {
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
                collection
        );
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<ClickHousePayload>> getWriterStateSerializer() {
        return new ClickHouseAsyncSinkSerializer();
    }
}

