package org.apache.flink.connector.clickhouse.sink;


import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.utils.writer.DataWriter;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.clickhouse.convertor.SupportsHeader;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
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
    public SinkWriter<InputT> createWriter(WriterInitContext writerInitContext) throws IOException {
        return restoreWriter(writerInitContext, Collections.emptyList());
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<ClickHousePayload>> restoreWriter(WriterInitContext writerInitContext, Collection<BufferedRequestState<ClickHousePayload>> collection) throws IOException {
        byte[] header = null;
        if (clickHouseFormat != null && clickHouseFormat.hasHeader()) {
            ElementConverter<InputT, ClickHousePayload> converter = getElementConverter();
            if (converter == null || (!(converter instanceof SupportsHeader))) {
                // unable to compute the header. Generate an error
                throw new IllegalStateException("A converter that supports headers is required for the format " + clickHouseFormat.name());
            }

            SupportsHeader headerConvertor = (SupportsHeader) converter;
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataWriter dataWriter = DataWriter.of(true, out);
            headerConvertor.header(dataWriter);
            header = out.toByteArray();
        }

        return new ClickHouseAsyncWriter<>(
                getElementConverter(),
                writerInitContext,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                clickHouseClientConfig,
                clickHouseFormat,
                header,
                collection
        );
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<ClickHousePayload>> getWriterStateSerializer() {
        return new ClickHouseAsyncSinkSerializer();
    }
}

