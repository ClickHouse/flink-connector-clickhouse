package org.apache.flink.connector.clickhouse.convertor;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;


public class ClickHouseConvertor<InputT> implements ElementConverter<InputT, ClickHousePayload> {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseConvertor.class);

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
            if (payload.endsWith("\n"))
                return new ClickHousePayload(payload.getBytes(StandardCharsets.UTF_8));
            return new ClickHousePayload((payload + "\n").getBytes());
        }
        if (type == Types.POJO) {
            // TODO Convert POJO to bytes
            try {
                byte[] payload = this.pojoConvertor.convert(o);
                return new ClickHousePayload(payload);
            } catch (Exception e) {
                return new ClickHousePayload(null);
            }
        }
        throw new IllegalArgumentException("unable to convert " + o + " to " + type);
    }

    @Override
    public void open(WriterInitContext context) {
        ElementConverter.super.open(context);
    }
}
