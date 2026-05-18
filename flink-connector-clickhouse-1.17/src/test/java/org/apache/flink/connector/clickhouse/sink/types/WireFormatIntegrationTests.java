package org.apache.flink.connector.clickhouse.sink.types;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseFormat;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.convertor.ColumnBinding;
import org.apache.flink.connector.clickhouse.convertor.DataMapper;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSinkSerializer;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.connector.test.FlinkClusterTests;
import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.connector.clickhouse.sink.ClickHouseSinkTestUtils.*;
import static org.apache.flink.connector.test.embedded.flink.EmbeddedFlinkClusterForTests.executeAsyncJob;

/**
 * Wire-format integration tests for the v0.2.0 Map-based payload design.
 *
 * <p>Covers five properties of the new {@code RowBinaryWithNamesAndTypes}
 * wire format and V1 backcompat:
 *
 * <ul>
 *   <li>{@link #reorderedBindingsLandInCorrectColumns()} — names+types header
 *       matches columns by name, not by position.</li>
 *   <li>{@link #nullValuePreservedAsNullOnNullableColumn()} — explicit null on a
 *       Nullable column is stored as NULL (the table DEFAULT does NOT fire).</li>
 *   <li>{@link #omittedColumnGetsDefaultForAllRows()} — columns missing from
 *       {@code bindings()} pick up the table DEFAULT for every row
 *       ({@code input_format_defaults_for_omitted_fields = 1}).</li>
 *   <li>{@link #v1StringRestoreFlushesLegacyBytes()} — V1 (pre-T7)
 *       {@code ENTRY_BYTES_ONLY} checkpoint blob is restorable in STRING mode and
 *       its raw bytes survive deserialization for ClickHouse ingest.</li>
 *   <li>{@link #v1TypedRestoreThrowsDrainFirst()} — V1 blob restored in typed
 *       mode throws an {@code IOException} with the drain-first message.</li>
 * </ul>
 */
public class WireFormatIntegrationTests extends FlinkClusterTests {

    static final int STREAM_PARALLELISM = 1;

    // ====================================================================
    // Test 1: reordered bindings → columns match by name
    // ====================================================================

    public static class ReorderRow implements Serializable {
        public int a;
        public String b;

        public ReorderRow() {}

        public ReorderRow(int a, String b) {
            this.a = a;
            this.b = b;
        }
    }

    /** {@code bindings()} returns columns in REVERSE order vs the table. */
    public static class ReversedOrderMapper extends DataMapper<ReorderRow> {
        @Override
        public void toMap(ReorderRow input, Map<String, Object> map) {
            map.put("a_key", input.a);
            map.put("b_key", input.b);
        }

        @Override
        public List<ColumnBinding> bindings() {
            return List.of(
                ColumnBinding.scalar("b_key", "col_b", ClickHouseDataType.String),
                ColumnBinding.scalar("a_key", "col_a", ClickHouseDataType.Int32)
            );
        }
    }

    @Test
    void reorderedBindingsLandInCorrectColumns() throws Exception {
        String tableName = "reorder_test";

        ClickHouseServerForTests.executeSql(
            "CREATE TABLE `" + getDatabase() + "`.`" + tableName + "` ("
                + "col_a Int32, col_b String"
                + ") ENGINE=MergeTree ORDER BY col_a");

        ClickHouseAsyncSink<ReorderRow> sink = buildSink(
            new ClickHouseConvertor<>(ReorderRow.class, new ReversedOrderMapper()),
            tableName);

        List<ReorderRow> rows = new ArrayList<>();
        rows.add(new ReorderRow(1, "one"));
        rows.add(new ReorderRow(2, "two"));

        runJob(sink, rows, tableName, 2);

        List<GenericRecord> records = ClickHouseServerForTests.extractAllData(
            getDatabase(), tableName, "col_a");
        Assertions.assertEquals(2, records.size());
        Assertions.assertEquals(1, records.get(0).getInteger("col_a"));
        Assertions.assertEquals("one", records.get(0).getString("col_b"));
        Assertions.assertEquals(2, records.get(1).getInteger("col_a"));
        Assertions.assertEquals("two", records.get(1).getString("col_b"));
    }

    // ====================================================================
    // Test 2: Nullable column + null stays null (NOT substituted by DEFAULT)
    // ====================================================================

    public static class NullableRow implements Serializable {
        public Integer status;
        public String name;

        public NullableRow() {}

        public NullableRow(Integer status, String name) {
            this.status = status;
            this.name = name;
        }
    }

    public static class NullableRowMapper extends DataMapper<NullableRow> {
        @Override
        public void toMap(NullableRow r, Map<String, Object> map) {
            map.put("status", r.status);
            map.put("name", r.name);
        }

        @Override
        public List<ColumnBinding> bindings() {
            return List.of(
                ColumnBinding.scalar("status", "status", ClickHouseDataType.Int32, true, false),
                ColumnBinding.scalar("name", "name", ClickHouseDataType.String)
            );
        }
    }

    @Test
    void nullValuePreservedAsNullOnNullableColumn() throws Exception {
        String tableName = "nullable_default_test";

        ClickHouseServerForTests.executeSql(
            "CREATE TABLE `" + getDatabase() + "`.`" + tableName + "` ("
                + "  status Nullable(Int32) DEFAULT 99, "
                + "  name String"
                + ") ENGINE=MergeTree ORDER BY name");

        ClickHouseAsyncSink<NullableRow> sink = buildSink(
            new ClickHouseConvertor<>(NullableRow.class, new NullableRowMapper()),
            tableName);

        List<NullableRow> rows = new ArrayList<>();
        rows.add(new NullableRow(1, "explicit"));
        rows.add(new NullableRow(null, "stayed_null"));

        runJob(sink, rows, tableName, 2);

        List<GenericRecord> records = ClickHouseServerForTests.extractAllData(
            getDatabase(), tableName, "name");
        Assertions.assertEquals(2, records.size());

        GenericRecord explicit = records.get(0);
        GenericRecord stayedNull = records.get(1);
        Assertions.assertEquals("explicit", explicit.getString("name"));
        Assertions.assertEquals(1, explicit.getInteger("status"));
        Assertions.assertEquals("stayed_null", stayedNull.getString("name"));
        Assertions.assertFalse(stayedNull.hasValue("status"),
            "Explicit null on a Nullable column is stored as NULL "
            + "(the DEFAULT clause does NOT fire for explicit nulls)");
    }

    // ====================================================================
    // Test 3: column omitted from bindings → table DEFAULT for whole batch
    // ====================================================================

    public static class OmissionRow implements Serializable {
        public int id;

        public OmissionRow() {}

        public OmissionRow(int id) {
            this.id = id;
        }
    }

    /** {@code bindings()} omits {@code extra_col} entirely. */
    public static class OmissionMapper extends DataMapper<OmissionRow> {
        @Override
        public void toMap(OmissionRow r, Map<String, Object> map) {
            map.put("id", r.id);
        }

        @Override
        public List<ColumnBinding> bindings() {
            return List.of(
                ColumnBinding.scalar("id", "id", ClickHouseDataType.Int32)
                // extra_col deliberately omitted
            );
        }
    }

    @Test
    void omittedColumnGetsDefaultForAllRows() throws Exception {
        String tableName = "omission_test";

        ClickHouseServerForTests.executeSql(
            "CREATE TABLE `" + getDatabase() + "`.`" + tableName + "` ("
                + "  id Int32, "
                + "  extra_col String DEFAULT 'omg'"
                + ") ENGINE=MergeTree ORDER BY id");

        ClickHouseAsyncSink<OmissionRow> sink = buildSink(
            new ClickHouseConvertor<>(OmissionRow.class, new OmissionMapper()),
            tableName);

        List<OmissionRow> rows = new ArrayList<>();
        rows.add(new OmissionRow(1));
        rows.add(new OmissionRow(2));

        runJob(sink, rows, tableName, 2);

        List<GenericRecord> records = ClickHouseServerForTests.extractAllData(
            getDatabase(), tableName, "id");
        Assertions.assertEquals(2, records.size());
        for (GenericRecord rec : records) {
            Assertions.assertEquals("omg", rec.getString("extra_col"),
                "Omitted column should pick up DEFAULT for every row");
        }
    }

    // ====================================================================
    // Test 4: V1 STRING-mode restore — bytes survive → ClickHouse accepts
    // ====================================================================

    @Test
    void v1StringRestoreFlushesLegacyBytes() throws Exception {
        String tableName = "v1_restore_csv_test";
        ClickHouseServerForTests.executeSql(
            "CREATE TABLE `" + getDatabase() + "`.`" + tableName + "` ("
                + "  id Int32, "
                + "  name String"
                + ") ENGINE=MergeTree ORDER BY id");

        byte[] csv1 = "1,alpha\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] csv2 = "2,beta\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] blob = synthesizeV1Blob(csv1, csv2);

        ClickHouseAsyncSinkSerializer ser = new ClickHouseAsyncSinkSerializer(true);
        BufferedRequestState<ClickHousePayload> state = ser.deserialize(1, blob);

        List<RequestEntryWrapper<ClickHousePayload>> entries =
            new ArrayList<>(state.getBufferedRequestEntries());
        Assertions.assertEquals(2, entries.size());

        // Confirm restored entries are raw and need rehydration. This mirrors the
        // writer's input on restart: it would call setCachedBytes from RAW_KEY
        // before send.
        for (RequestEntryWrapper<ClickHousePayload> w : entries) {
            ClickHousePayload p = w.getRequestEntry();
            Assertions.assertTrue(p.isRaw());
            Assertions.assertTrue(p.needsRehydration());
            p.setCachedBytes((byte[]) p.getData().get(ClickHousePayload.RAW_KEY));
        }

        // Send the restored raw bytes via the production client.
        ClickHouseClientConfig clientConfig = new ClickHouseClientConfig(
            getServerURL(), getUsername(), getPassword(), getDatabase(), tableName);
        try (Client client = clientConfig.createClient()) {
            InsertSettings insertSettings = new InsertSettings();
            CompletableFuture<InsertResponse> future = client.insert(tableName, out -> {
                for (RequestEntryWrapper<ClickHousePayload> w : entries) {
                    out.write(w.getRequestEntry().getCachedBytes());
                }
                out.close();
            }, ClickHouseFormat.CSV, insertSettings);
            future.get();
        }

        List<GenericRecord> records = ClickHouseServerForTests.extractAllData(
            getDatabase(), tableName, "id");
        Assertions.assertEquals(2, records.size());
        Assertions.assertEquals(1, records.get(0).getInteger("id"));
        Assertions.assertEquals("alpha", records.get(0).getString("name"));
        Assertions.assertEquals(2, records.get(1).getInteger("id"));
        Assertions.assertEquals("beta", records.get(1).getString("name"));
    }

    // ====================================================================
    // Test 5: V1 typed-mode restore aborts with drain-first message
    // ====================================================================

    public static class TypedRow implements Serializable {
        public int a;
        public String b;
    }

    public static class TypedRowMapper extends DataMapper<TypedRow> {
        @Override
        public void toMap(TypedRow r, Map<String, Object> map) {
            map.put("a", r.a);
            map.put("b", r.b);
        }

        @Override
        public List<ColumnBinding> bindings() {
            return List.of(
                ColumnBinding.scalar("a", "col_a", ClickHouseDataType.Int32),
                ColumnBinding.scalar("b", "col_b", ClickHouseDataType.String)
            );
        }
    }

    @Test
    void v1TypedRestoreThrowsDrainFirst() throws Exception {
        String tableName = "v1_typed_restore_abort_test";
        ClickHouseServerForTests.executeSql(
            "CREATE TABLE `" + getDatabase() + "`.`" + tableName + "` ("
                + "col_a Int32, col_b String"
                + ") ENGINE=MergeTree ORDER BY col_a");

        // Build the production sink end-to-end (mirrors what an operator would wire
        // up). Pull the sink's own state serializer to make sure the rejection path
        // is reachable via the production wiring, not just through a freshly
        // constructed serializer.
        ClickHouseAsyncSink<TypedRow> sink = buildSink(
            new ClickHouseConvertor<>(TypedRow.class, new TypedRowMapper()),
            tableName);

        byte[] blob = synthesizeV1Blob(new byte[]{1, 2, 3});
        ClickHouseAsyncSinkSerializer ser =
            (ClickHouseAsyncSinkSerializer) sink.getWriterStateSerializer();

        IOException ex = Assertions.assertThrows(IOException.class,
            () -> ser.deserialize(1, blob));
        Assertions.assertTrue(
            ex.getMessage().contains("Drain the previous sink before upgrading"),
            "Expected drain-first message; got: " + ex.getMessage());
    }

    // ====================================================================
    // Helpers
    // ====================================================================

    /** Construct a sink with the standard test-batch tuning + a typed converter. */
    private <T> ClickHouseAsyncSink<T> buildSink(ClickHouseConvertor<T> convertor, String tableName) {
        ClickHouseClientConfig clientConfig = new ClickHouseClientConfig(
            getServerURL(), getUsername(), getPassword(), getDatabase(), tableName);
        return ClickHouseAsyncSink.<T>builder()
            .setElementConverter(convertor)
            .setMaxBatchSize(MAX_BATCH_SIZE)
            .setMaxInFlightRequests(MAX_IN_FLIGHT_REQUESTS)
            .setMaxBufferedRequests(MAX_BUFFERED_REQUESTS)
            .setMaxBatchSizeInBytes(MAX_BATCH_SIZE_IN_BYTES)
            .setMaxTimeInBufferMS(MAX_TIME_IN_BUFFER_MS)
            .setMaxRecordSizeInBytes(MAX_RECORD_SIZE_IN_BYTES)
            .setClickHouseClientConfig(clientConfig)
            .build();
    }

    /** Run a small Flink mini-cluster job that ships a list through the sink. */
    private <T> void runJob(ClickHouseAsyncSink<T> sink, List<T> rows,
                            String tableName, int expectedRows) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(STREAM_PARALLELISM);
        DataStream<T> stream = env.fromCollection(rows);
        stream.sinkTo(sink);
        int inserted = executeAsyncJob(env, tableName, 10, expectedRows);
        Assertions.assertEquals(expectedRows, inserted);
    }

    /** V1 framing identical to parent {@code AsyncSinkWriterStateSerializer}. */
    private static byte[] synthesizeV1Blob(byte[]... entryPayloads) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            out.writeLong(-1L);                          // DATA_IDENTIFIER (parent constant)
            out.writeInt(entryPayloads.length);
            for (byte[] payload : entryPayloads) {
                ByteArrayOutputStream body = new ByteArrayOutputStream();
                try (DataOutputStream b = new DataOutputStream(body)) {
                    b.writeInt(1);                       // ENTRY_BYTES_ONLY marker
                    b.writeInt(payload.length);
                    b.write(payload);
                }
                out.writeLong(body.size());              // per-entry size prefix
                out.write(body.toByteArray());
            }
        }
        return baos.toByteArray();
    }
}
