package org.apache.flink.connector.clickhouse.convertor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

public abstract class POJOConvertor<InputT> implements Serializable {
    public abstract void instrument(OutputStream out, InputT input) throws IOException;

    public byte[] convert(InputT input) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        instrument(out, input);
        return out.toByteArray();
    }
}
