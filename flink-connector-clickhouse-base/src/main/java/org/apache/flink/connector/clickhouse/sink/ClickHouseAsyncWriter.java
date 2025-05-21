package org.apache.flink.connector.clickhouse.sink;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.data.ClickHouseFormat;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class ClickHouseAsyncWriter<InputT> extends AsyncSinkWriter<InputT, ClickHousePayload> {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseAsyncWriter.class);

    private final ClickHouseClientConfig clickHouseClientConfig;
    private ClickHouseFormat clickHouseFormat = null;

    public ClickHouseAsyncWriter(ElementConverter<InputT, ClickHousePayload> elementConverter,
                                 WriterInitContext context,
                                 int maxBatchSize,
                                 int maxInFlightRequests,
                                 int maxBufferedRequests,
                                 long maxBatchSizeInBytes,
                                 long maxTimeInBufferMS,
                                 long maxRecordSizeInBytes,
                                 ClickHouseClientConfig clickHouseClientConfig,
                                 ClickHouseFormat clickHouseFormat,
                                 Collection<BufferedRequestState<ClickHousePayload>> state) {
        super(elementConverter,
              context,
              AsyncSinkWriterConfiguration.builder()
                    .setMaxBatchSize(maxBatchSize)
                    .setMaxBatchSizeInBytes(maxBatchSizeInBytes)
                    .setMaxInFlightRequests(maxInFlightRequests)
                    .setMaxBufferedRequests(maxBufferedRequests)
                    .setMaxTimeInBufferMS(maxTimeInBufferMS)
                    .setMaxRecordSizeInBytes(maxRecordSizeInBytes)
                    .build(),
              state);
        this.clickHouseClientConfig = clickHouseClientConfig;
        this.clickHouseFormat = clickHouseFormat;
    }

    @Override
    protected long getSizeInBytes(ClickHousePayload clickHousePayload) {
        return clickHousePayload.getPayloadLength();
    }

    @Override
    protected void submitRequestEntries(List<ClickHousePayload> requestEntries, ResultHandler<ClickHousePayload> resultHandler) {
        LOG.info("Submitting request entries...");
        System.out.println("Submitting request entries...");
        AtomicInteger totalSizeSend = new AtomicInteger();
        Client chClient = this.clickHouseClientConfig.createClient();
        String tableName = clickHouseClientConfig.getTableName();
        // TODO: get from constructor or ClickHousePayload need to think what is the best way
        ClickHouseFormat format = null;
        if (clickHouseFormat == null) {
            // this not define lets try to get it from ClickHousePayload in case  of POJO can be RowBinary or RowBinaryWithDefaults
        } else {
            format = clickHouseFormat;
        }
        try {
            CompletableFuture<InsertResponse> response = chClient.insert(tableName, out -> {
                for (ClickHousePayload requestEntry : requestEntries) {
                    byte[] payload = requestEntry.getPayload();
                    totalSizeSend.addAndGet(payload.length);
                    out.write(payload);
                }
                LOG.info("Data that will be send to ClickHouse in bytes {} and the amount of records {}.", totalSizeSend.get(), requestEntries.size());
                out.close();
            }, format, new InsertSettings().setOption(ClientConfigProperties.ASYNC_OPERATIONS.getKey(), "true"));
            response.whenComplete((insertResponse, throwable) -> {
                if (throwable != null) {
                    handleFailedRequest(requestEntries, resultHandler, throwable);
                } else {
                    handleSuccessfulRequest(resultHandler, insertResponse);
                }
            }).join();
        } catch (Exception e) {
            LOG.error("Error: ", e);
        }
        LOG.info("Finished submitting request entries.");
    }

    private void handleSuccessfulRequest(
            ResultHandler<ClickHousePayload> resultHandler, InsertResponse response) {
        LOG.info("Successfully completed submitting request. Response [written rows {}, time took to insert {}, written bytes {}, query_id {}]",
                response.getWrittenRows(),
                response.getServerTime(),
                response.getWrittenBytes(),
                response.getQueryId()
        );
        resultHandler.complete();
    }

    private void handleFailedRequest(
            List<ClickHousePayload> requestEntries,
            ResultHandler<ClickHousePayload> resultHandler,
            Throwable error) {
        // TODO extract from error if we can retry
        error.printStackTrace();

    }

}
