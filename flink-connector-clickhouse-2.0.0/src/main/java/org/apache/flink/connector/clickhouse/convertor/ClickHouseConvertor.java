package org.apache.flink.connector.clickhouse.convertor;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;

import com.clickhouse.utils.writer.DataWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public class ClickHouseConvertor<InputT>
        implements ElementConverter<InputT, ClickHousePayload> {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseConvertor.class);
    private static final long serialVersionUID = 3L;

    enum Types { STRING, POJO }
    private final Types type;
    private final DataMapper<InputT> mapper;

    /** Reused across apply() calls — safe because Flink calls apply() single-threaded
     *  per writer and toByteArray() returns a defensive copy. */
    private transient ByteArrayOutputStream buffer;
    private transient DataWriter dataWriter;
    private transient List<ColumnBinding> cachedBindings;

    public ClickHouseConvertor(Class<InputT> clazz) {
        Objects.requireNonNull(clazz, "clazz must not be null");
        if (clazz != String.class) {
            throw new IllegalArgumentException(
                "POJO input requires a DataMapper; use the (Class, DataMapper) constructor");
        }
        this.type = Types.STRING;
        this.mapper = null;
    }

    public ClickHouseConvertor(Class<InputT> clazz, DataMapper<InputT> mapper) {
        Objects.requireNonNull(clazz, "clazz must not be null");
        Objects.requireNonNull(mapper, "mapper must not be null");
        this.type = Types.POJO;
        this.mapper = mapper;
    }

    @Override
    public void open(WriterInitContext context) {
        if (mapper != null) {
            this.cachedBindings = List.copyOf(mapper.bindings());
            // Reserved-key collision check runs BEFORE super.open so a unit test can
            // pass a null context and still validate the error path.
            for (ColumnBinding b : cachedBindings) {
                if (ClickHousePayload.RAW_KEY.equals(b.mapKey)) {
                    throw new IllegalStateException(
                        "DataMapper.bindings() uses reserved Map key: " + ClickHousePayload.RAW_KEY
                        + " (binding for column '" + b.columnName + "')");
                }
            }
        }
        ElementConverter.super.open(context);
        this.buffer = new ByteArrayOutputStream();
        this.dataWriter = DataWriter.of(buffer);
    }

    @Override
    public ClickHousePayload apply(InputT o, SinkWriter.Context context) {
        if (o == null) return null;

        if (type == Types.STRING) {
            String s = (String) o;
            if (s.isEmpty()) return ClickHousePayload.ofRaw(new byte[0]);
            byte[] bytes = s.endsWith("\n")
                    ? s.getBytes(StandardCharsets.UTF_8)
                    : (s + "\n").getBytes(StandardCharsets.UTF_8);
            return ClickHousePayload.ofRaw(bytes);
        }

        try {
            ClickHousePayload payload = ClickHousePayload.ofEmpty();
            mapper.toMap(o, payload.getData());
            buffer.reset();
            for (ColumnBinding b : cachedBindings) {
                dataWriter.writeValue(payload.getData().get(b.mapKey), b.column);
            }
            payload.setCachedBytes(buffer.toByteArray());
            return payload;
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert input to ClickHouse payload", e);
        }
    }

    public boolean isStringMode() { return type == Types.STRING; }
    public List<ColumnBinding> getBindings() { return cachedBindings; }
}
