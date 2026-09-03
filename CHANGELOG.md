## Unreleased — Table API / Flink SQL sink

### New

- Flink SQL / Table API sink: `'connector' = 'clickhouse'`, insert-only. At planning time the
  connector reads the target table's columns from ClickHouse and validates the Flink schema
  against them, so mismatches fail at job submission. Options (`sink.buffer-flush.*`,
  `sink.max-in-flight-requests`, `sink.max-buffered-requests`, `sink.record.max-bytes`,
  `sink.parallelism`, `sink.max-retries`, `sink.batch-failure-strategy`, `sink.timezone`,
  `sink.ignore-unknown-flink-columns`, `clickhouse.client.*` / `clickhouse.server.*`
  passthrough) and the type mapping are documented in the README's "Table API" section.
- `ClickHouseClientConfig`: a constructor taking a `RetryPolicy` that does not ping at
  construction, `copy()`, and `createPlanningClient()`.
- `ClickHouseSinkDefaults`: the batching defaults shared by the DataStream builder and the SQL options.

### Fixed

- `DataWriter` sized `Nullable(FixedString(n))` values by the column's estimated length (1)
  instead of `n`.

### Checkpoint migration

- Checkpoint entry format V3: strings and map keys are int-length-prefixed UTF-8, so values
  above `writeUTF`'s 64 KB limit checkpoint safely. Checkpoints written by 0.2.0 (V2) restore
  transparently; 0.1.x STRING-mode checkpoints still restore as before.
- **Rolling back to 0.2.0 from a V3 checkpoint is NOT supported**: 0.2.0 fails on restore with
  `Unknown entry marker: 3`. Drain the sink before downgrading: stop the source, wait for the
  buffer to flush, take a checkpoint with zero in-flight entries, then roll back.

## 0.2.0 — Map-based payload, RowBinaryWithNamesAndTypes

### Breaking changes

- `POJOConvertor<T>` is replaced by `DataMapper<T>`. The new class has two abstract
  methods: `toMap(input, map)` and `bindings()`. See migration steps below.
- `ClickHousePayload<T>` is no longer generic. The class is now `ClickHousePayload`,
  with a `Map<String, Object> data` field carrying the canonical state.
- Per-row defaults via `RowBinaryWithDefaults` are no longer supported. Use
  `Nullable(...)` columns + `null` in the Map (the explicit null is stored as NULL),
  or omit columns entirely from `bindings()` for whole-batch DEFAULT substitution
  (server-side `input_format_defaults_for_omitted_fields = 1` is set automatically).
- Typed (POJO) sinks now insert as `RowBinaryWithNamesAndTypes`; the
  `setClickHouseFormat(...)` config is ignored in typed mode (warns at open()).
- Builder method `setPOJOConvertor(...)` is removed; the convertor still hangs off
  `setElementConverter(new ClickHouseConvertor<>(MyClass.class, new MyDataMapper()))`.

### Checkpoint migration

- **STRING-mode sinks** restoring v0.1.x checkpoints: works transparently. Legacy
  bytes are wrapped under a reserved Map key and flushed verbatim using the
  sink's currently configured format.
- **Typed (POJO) sinks** restoring v0.1.x checkpoints: NOT supported. The wire format
  has changed. Drain the previous sink before upgrading: stop the source, wait
  for the buffer to flush, take a fresh checkpoint with zero in-flight entries,
  then upgrade.

### Migrating a POJOConvertor to a DataMapper

Before:

    public class MyConvertor extends POJOConvertor<MyPOJO> {
        public MyConvertor(boolean schemaHasDefaults) { super(schemaHasDefaults); }
        @Override public void instrument(DataWriter dw, MyPOJO input) throws IOException {
            dw.writeInt8(input.getByteVal(),  false, ClickHouseDataType.Int8,   false, "byte_col");
            dw.writeString(input.getName(),   false, ClickHouseDataType.String, false, "name");
            dw.writeDecimal(input.getMoney(), false, ClickHouseDataType.Decimal, false, "money", 10, 5);
        }
    }

After:

    public class MyConvertor extends DataMapper<MyPOJO> {
        @Override public void toMap(MyPOJO input, Map<String, Object> map) {
            map.put("byte_val", input.getByteVal());
            map.put("name",     input.getName());
            map.put("money",    input.getMoney());
        }
        @Override public List<ColumnBinding> bindings() {
            return List.of(
                ColumnBinding.scalar ("byte_val", "byte_col", ClickHouseDataType.Int8),
                ColumnBinding.scalar ("name",     "name",     ClickHouseDataType.String),
                ColumnBinding.decimal("money",    "money",    10, 5)
            );
        }
    }

## 0.1.4
* Add JSON type support https://github.com/ClickHouse/flink-connector-clickhouse/pull/106
* Enhance instrument api https://github.com/ClickHouse/flink-connector-clickhouse/pull/99
## 0.1.3
* Added an option to configure the client with server-side parameters. https://github.com/ClickHouse/flink-connector-clickhouse/pull/85 
* Added check if ClickHouse is alive before start. https://github.com/ClickHouse/flink-connector-clickhouse/issues/76 
## 0.1.2
* Fix serialize state  https://github.com/ClickHouse/flink-connector-clickhouse/pull/64
## 0.1.1
* Add support for Array, Map, Tuple #57
* Add support in FixedString, Date/Date32/DateTime/DateTime64, Uint8/16/32/64/128/256, Decimal, UUID
## 0.1.0
* ClickHouse Sink supports Apache Flink 1.17+