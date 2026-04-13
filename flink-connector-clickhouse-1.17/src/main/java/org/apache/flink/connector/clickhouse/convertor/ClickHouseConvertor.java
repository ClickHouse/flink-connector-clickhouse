package org.apache.flink.connector.clickhouse.convertor;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;


public class ClickHouseConvertor<InputT> implements ElementConverter<InputT, ClickHousePayload> {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseConvertor.class);
    private static final long serialVersionUID = 1L;

    POJOConvertor<InputT> pojoConvertor = null;
    enum Types {
        STRING,
        POJO,
    }
    private final Types type;

    public ClickHouseConvertor(Class<?> clazz) {
        if (clazz == null) {
            throw new IllegalArgumentException("clazz must not be not null");
        }
        if (clazz == String.class) {
            type = Types.STRING;

        } else {
            type = Types.POJO;
            // lets register it

        }
    }

    public ClickHouseConvertor(Class<?> clazz, POJOConvertor<InputT> pojoConvertor) {
        if (clazz == null) {
            throw new IllegalArgumentException("clazz must not be not null");
        } else {
            type = Types.POJO;
            this.pojoConvertor = pojoConvertor;
        }
    }

    @Override
    public ClickHousePayload apply( InputT o, SinkWriter.Context context) {
        if (o == null) {
            // we need to skip it
            return null;
        }
        //
        if (o instanceof String && type == Types.STRING) {
            String payload = o.toString();
            if (payload.isEmpty()) {
                return new ClickHousePayload(null);
            }
            byte[] bytes;
            if (payload.endsWith("\n"))
                bytes = payload.getBytes(StandardCharsets.UTF_8);
            else
                bytes = (payload + "\n").getBytes(StandardCharsets.UTF_8);
            return new ClickHousePayload(bytes, (java.io.Serializable) o);
        }
        if (type == Types.POJO) {
            try {
                byte[] payload = this.pojoConvertor.convert(o);
                java.io.Serializable originalInput = (o instanceof java.io.Serializable) ? (java.io.Serializable) o : null;
                return new ClickHousePayload(payload, originalInput);
            } catch (Exception e) {
                throw new RuntimeException("Failed to convert POJO to ClickHouse payload", e);
            }
        }
        throw new IllegalArgumentException("unable to convert " + o + " to " + type);
    }

    @Override
    public void open(Sink.InitContext context) {
        ElementConverter.super.open(context);
    }
}
