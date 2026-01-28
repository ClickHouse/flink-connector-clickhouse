package org.apache.flink.connector.clickhouse.convertor;

import com.clickhouse.utils.writer.DataWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

public abstract class POJOConvertor<InputT> implements Serializable {
    private final boolean schemaHasDefaults;

    public POJOConvertor(boolean schemaHasDefaults) {
        this.schemaHasDefaults = schemaHasDefaults;
    }

    public abstract void instrument(DataWriter dataWriter, InputT input) throws IOException;

    public byte[] convert(InputT input) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataWriter dataWriter = DataWriter.of(this.isSchemaHasDefaults(), out);
        instrument(dataWriter, input);
        return out.toByteArray();
    }

    public boolean isSchemaHasDefaults() {
        return schemaHasDefaults;
    }
}
