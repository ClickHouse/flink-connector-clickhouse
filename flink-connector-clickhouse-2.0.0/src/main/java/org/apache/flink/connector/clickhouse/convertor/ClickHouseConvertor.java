package org.apache.flink.connector.clickhouse.convertor;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Objects;


public class ClickHouseConvertor<InputT> implements ElementConverter<InputT, ClickHousePayload<InputT>> {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseConvertor.class);
    private static final long serialVersionUID = 2L;

    POJOConvertor<InputT> pojoConvertor = null;
    enum Types {
        STRING,
        POJO,
    }
    private final Types type;
    private final TypeInformation<InputT> inputTypeInfo;

    public ClickHouseConvertor(Class<InputT> clazz) {
        Objects.requireNonNull(clazz, "clazz must not be null");
        this.inputTypeInfo = TypeInformation.of(clazz);
        this.type = (clazz == String.class) ? Types.STRING : Types.POJO;
    }

    public ClickHouseConvertor(Class<InputT> clazz, POJOConvertor<InputT> pojoConvertor) {
        Objects.requireNonNull(clazz, "clazz must not be null");
        this.inputTypeInfo = TypeInformation.of(clazz);
        this.type = Types.POJO;
        this.pojoConvertor = pojoConvertor;
    }

    /** Returns the {@link TypeInformation} for {@code InputT}, used for state serialization. */
    public TypeInformation<InputT> getInputTypeInfo() {
        return inputTypeInfo;
    }

    @Override
    public ClickHousePayload<InputT> apply(InputT o, SinkWriter.Context context) {
        if (o == null) {
            return null;
        }
        if (o instanceof String && type == Types.STRING) {
            String payload = (String) o;
            if (payload.isEmpty()) {
                return new ClickHousePayload<>((byte[]) null);
            }
            byte[] bytes = payload.endsWith("\n")
                    ? payload.getBytes(StandardCharsets.UTF_8)
                    : (payload + "\n").getBytes(StandardCharsets.UTF_8);
            return new ClickHousePayload<>(bytes, o);
        }
        if (type == Types.POJO) {
            try {
                byte[] payload = this.pojoConvertor.convert(o);
                return new ClickHousePayload<>(payload, o);
            } catch (Exception e) {
                throw new RuntimeException("Failed to convert POJO to ClickHouse payload", e);
            }
        }
        throw new IllegalArgumentException("unable to convert " + o + " to " + type);
    }

    @Override
    public void open(WriterInitContext context) {
        ElementConverter.super.open(context);
    }
}
