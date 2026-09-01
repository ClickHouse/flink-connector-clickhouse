<div align="center">
<p><img src="https://github.com/ClickHouse/clickhouse-js/blob/a332672bfb70d54dfd27ae1f8f5169a6ffeea780/.static/logo.svg" width="200px" align="center"></p>
<h1>ClickHouse Flink Connector</h1>
</div>

Table of Contents
* [About The Project](#about-the-project)
* [Supported Flink Versions](#supported-flink-versions)
* [Installation](#installation)
* [DataStream API](#dataStream-api)
    * [Snippets](#snippet)
    * [Examples](#example)
* [Table API](#table-api)
    * [Snippets](#snippet-1)
    * [Examples](#example-1)
* [Supported ClickHouse Types](#supported-clickHouse-types)
* [Configuration Options](#configuration-options)
  * [Client Configuration](#client-configuration)
  * [Sink Configuration](#sink-configuration)
  * [Sink Metrics](#sink-metrics)
* [Limitations](#limitations)
* [Contributing](#contributing)

## About The Project

This is a repo of ClickHouse official Apache Flink Connector supported by the ClickHouse team.
The connector supports two main Apache Flink APIs: 
- DataStream API
- Table API / Flink SQL (sink only, insert-only changelog)

## Supported Flink Versions

| Version | Dependency                       | ClickHouse Client Version | Required Java |
|---------|----------------------------------|---------------------------|---------------|
| latest  | flink-connector-clickhouse-2.0.0 | 0.9.5                     | Java 17+      |
| 2.0.1   | flink-connector-clickhouse-2.0.0 | 0.9.5                     | Java 17+      |
| 2.0.0   | flink-connector-clickhouse-2.0.0 | 0.9.5                     | Java 17+      |
| 1.20.2  | flink-connector-clickhouse-1.17  | 0.9.5                     | Java 11+      |
| 1.19.3  | flink-connector-clickhouse-1.17  | 0.9.5                     | Java 11+      |
| 1.18.1  | flink-connector-clickhouse-1.17  | 0.9.5                     | Java 11+      |
| 1.17.2  | flink-connector-clickhouse-1.17  | 0.9.5                     | Java 11+      |

## Installation

### For Flink 2.0.0+

Maven 

```xml
<dependency>
    <groupId>com.clickhouse.flink</groupId>
    <artifactId>flink-connector-clickhouse-2.0.0</artifactId>
    <version>0.2.0</version>
    <classifier>all</classifier>
</dependency>
```

### For Flink 1.17+ 

Maven 

```xml
<dependency>
    <groupId>com.clickhouse.flink</groupId>
    <artifactId>flink-connector-clickhouse-1.17</artifactId>
    <version>0.2.0</version>
    <classifier>all</classifier>
</dependency>
```

## DataStream API

### Snippet

Configure ClickHouseClient 

```java
ClickHouseClientConfig clickHouseClientConfig = new ClickHouseClientConfig(url, username, password, database, tableName);
```
If you are planning to insert RAW CSV data as is 

Create a ClickHouseConvertor

```java
ClickHouseConvertor<String> convertorString = new ClickHouseConvertor<>(String.class);
```

Build the sink (optional knobs have sensible defaults — set only what you need):

```java
ClickHouseAsyncSink<String> csvSink = ClickHouseAsyncSink.<String>builder()
        .setElementConverter(convertorString)
        .setClickHouseClientConfig(clickHouseClientConfig)
        .setClickHouseFormat(ClickHouseFormat.CSV)
        .setMaxBatchSize(MAX_BATCH_SIZE)
        .setMaxInFlightRequests(MAX_IN_FLIGHT_REQUESTS)
        .setMaxBufferedRequests(MAX_BUFFERED_REQUESTS)
        .setMaxBatchSizeInBytes(MAX_BATCH_SIZE_IN_BYTES)
        .setMaxTimeInBufferMS(MAX_TIME_IN_BUFFER_MS)
        .setMaxRecordSizeInBytes(MAX_RECORD_SIZE_IN_BYTES)
        .build();
```

Finally, connect your DataStream to the sink.

```java
data.sinkTo(csvSink);
```

More examples and snippets can be found in our tests [flink-connector-clickhouse-1.17](flink-connector-clickhouse-1.17/src/test/java/org/apache/flink/connector/clickhouse/sink) and [flink-connector-clickhouse-2.0.0](flink-connector-clickhouse-2.0.0/src/test/java/org/apache/flink/connector/clickhouse/sink) 

### Example

We have created maven based example for easy start with ClickHouse Sink 
Different versions for Flink 

**Java (Maven)**
- [Flink 1.17+](examples/maven/flink-v1.7/covid)
- [Flink 2.0.0+](examples/maven/flink-v2/covid)

**Scala (sbt)**
- [Flink 1.17+](examples/sbt/flink-v1.7/covid)
- [Flink 2.0.0+](examples/sbt/flink-v2/covid)

For more detailed instructions, see the [Example Guide](examples#readme)

## Table API

The connector registers itself as the `clickhouse` SQL connector: create a table with
`'connector' = 'clickhouse'` and `INSERT INTO` it from Flink SQL or the Table API
(`TableDescriptor.forConnector("clickhouse")`). The sink is insert-only: append queries work,
update-producing queries (e.g. a plain `GROUP BY` aggregation) are rejected by the planner —
use `ReplacingMergeTree`/`AggregatingMergeTree` plus `FINAL`/`argMax` for upsert-style needs.

At planning time the connector reads the real column types from the target ClickHouse table
and validates the Flink schema against them, so typos, type mismatches, narrowing and
unsupported types fail at job submission with a precise message instead of at the first
flush. Columns are matched **by name, case-sensitively**; ClickHouse columns you leave out of
the Flink schema get their server-side `DEFAULT`, or the type's default value (`0`/`''`/empty)
if they have none — the latter is logged as a warning at planning. A nullable Flink column can only target a
`Nullable(...)` ClickHouse column — declare columns `NOT NULL` when the target column isn't
`Nullable` (in Flink SQL, columns and collection elements are nullable unless declared
otherwise).

### Snippet

```sql
CREATE TABLE ch_events (
    event_id   BIGINT NOT NULL,
    amount     DECIMAL(18, 4) NOT NULL,
    created_at TIMESTAMP(3) NOT NULL
) WITH (
    'connector' = 'clickhouse',
    'url'       = 'http://localhost:8123',
    'username'  = 'default',
    'password'  = '',
    'database'  = 'analytics',
    'table'     = 'events'
    -- + optional batching/retry options, see below
);

INSERT INTO ch_events SELECT event_id, amount, created_at FROM kafka_src;
```

### Connector options

Connection options (required unless noted): `url`, `username`, `password` (defaults to `''`),
`database`, `table`.

**Batching / backpressure**

| Option | Default | DataStream equivalent |
|---|---|---|
| `sink.buffer-flush.max-rows` | `500` | `builder.setMaxBatchSize()` |
| `sink.buffer-flush.max-bytes` | `5mb` | `builder.setMaxBatchSizeInBytes()` |
| `sink.buffer-flush.interval` | `5s` | `builder.setMaxTimeInBufferMS()` |
| `sink.max-in-flight-requests` | `50` | `builder.setMaxInFlightRequests()` |
| `sink.max-buffered-requests` | `10000` | `builder.setMaxBufferedRequests()` |
| `sink.record.max-bytes` | `1mb` | `builder.setMaxRecordSizeInBytes()` |
| `sink.parallelism` | (query parallelism) | `sinkTo(sink).setParallelism(n)` |

**Reliability**

| Option | Default | DataStream equivalent |
|---|---|---|
| `sink.max-retries` | `-1` (retry forever) | `config.setRetryPolicy()` |
| `sink.batch-failure-strategy` | `stop-flink` (`drop-batch`) | `config.setBatchFailureStrategy()` |

**Type / compatibility**

| Option | Default | Effect |
|---|---|---|
| `sink.timezone` | `UTC` | zone in which `TIMESTAMP` (no time zone) wall-clock values are interpreted; DST gap wall clocks shift forward, ambiguous fall-back wall clocks take the earlier offset |
| `sink.ignore-unknown-flink-columns` | `false` | `true` drops Flink columns absent from the ClickHouse table instead of failing |

**Passthrough**: `clickhouse.client.<key>` options are forwarded to the ClickHouse client,
`clickhouse.server.<key>` become per-query server settings.

Flink `STRING` into a ClickHouse `JSON` column works out of the box — the connector enables
the client's JSON-as-string mode automatically exactly when a `JSON` column is mapped.

### Type mapping

Widening is implicit, unsigned targets are range-checked per record, and any other pair fails
at planning naming the column and both types.

| Flink SQL type | ClickHouse column types | Notes |
|---|---|---|
| `BOOLEAN` | `Bool` | |
| `TINYINT` | `Int8` or wider signed | |
| `SMALLINT` | `Int16`, `UInt8`, wider signed | |
| `INT` | `Int32`, `UInt16`, wider signed | |
| `BIGINT` | `Int64`, `UInt32`, `Int128`, `Int256` | |
| `DECIMAL(p, s)` | a `Decimal(p', s')` it fits; with `s = 0` also `Int128/256`, `UInt64/128/256` | `DECIMAL(20, 0)` covers the full `UInt64` range |
| `FLOAT` / `DOUBLE` | `Float32` (`FLOAT` only), `Float64` | |
| `CHAR` / `VARCHAR` / `STRING` | `String`, `FixedString(n)`, `UUID`, `JSON` | `FixedString` checked in bytes; `UUID` must be canonical text |
| `DATE` | `Date`, `Date32` | range-checked per record |
| `TIMESTAMP(p)` / `TIMESTAMP_LTZ(p)` | `DateTime` (`p = 0`), `DateTime64(s >= p)` | Flink's default `TIMESTAMP` is precision 6 — declare `TIMESTAMP(3)` for `DateTime64(3)`. `TIMESTAMP` is a wall clock in `sink.timezone`; `TIMESTAMP_LTZ` an instant |
| `ARRAY<t>` | `Array(T)` | only `Array(Nullable(T))` can carry nested NULLs |
| `MAP<k, v>` | `Map(K, V)` | string/integer keys except `UInt64`; values not `Nullable` |
| `MULTISET<t>` | `Map(T, UInt64)` | counts become the values |
| `ROW<...>` | `Tuple(...)` | positional; fields/elements not nullable |

Unsupported: `BINARY`/`VARBINARY`, `TIME`, `TIMESTAMP WITH TIME ZONE`, `INTERVAL`; ClickHouse
`Enum` (#43), `Variant` (#60), `Time` (#91), `IPv4/6`, `Dynamic`, geo — exclude such columns
and let server defaults fill them.

Everywhere: nullable Flink columns need `Nullable(...)` targets — except `Nullable(UInt8/16/32/64)`,
blocked until issue #144 — and composites must be `NOT NULL`. `LowCardinality` is transparent;
`SimpleAggregateFunction(f, T)` matches as `T`, top-level only.

Notes for SQL users:
- Operator names are planner-generated (e.g. `Sink: ch_events[3]`), which affects metric
  identifiers; `numRecordsSend` counts at flush, so retried batches double-count versus
  `SELECT count()`.
- A compiled plan does not freeze the ClickHouse mapping: an `ALTER TABLE` between planning
  and execution fails at the first flush (same exposure as the DataStream path).
- The community `itinycheng` connector also registers the `clickhouse` identifier; having
  both jars on the classpath fails with Flink's "multiple factories" error — keep only one.
- SQL gateways without ClickHouse network access can deploy in application mode, where
  planning runs on the JobManager inside the data plane.

### Example

See the round-trip integration test
[`ClickHouseTableApiIntegrationTests`](flink-connector-clickhouse-table/src/integrationTest/java/org/apache/flink/connector/clickhouse/table/ClickHouseTableApiIntegrationTests.java)
for a complete DDL + `INSERT INTO` example against a real ClickHouse.

## Supported ClickHouse Types

This is the DataStream (Java value) view; for the Flink SQL / Table API pairing rules see
[Type mapping](#type-mapping).

| Java Type       | ClickHouse Type | Supported | Serialize Method            |
|-----------------|-----------------|-----------|-----------------------------| 
| byte/Byte       | Int8            | ✅         | DataWriter.writeInt8        |
| short/Short     | Int16           | ✅         | DataWriter.writeInt16       |
| int/Integer     | Int32           | ✅         | DataWriter.writeInt32       |
| long/Long       | Int64           | ✅         | DataWriter.writeInt64       |
| BigInteger      | Int128          | ✅         | DataWriter.writeInt124      |
| BigInteger      | Int256          | ✅         | DataWriter.writeInt256      |
| short/Short     | UInt8           | ✅         | DataWriter.writeUInt8       |
| int/Integer     | UInt8           | ✅         | DataWriter.writeUInt8       |
| int/Integer     | UInt16          | ✅         | DataWriter.writeUInt16      |
| long/Long       | UInt32          | ✅         | DataWriter.writeUInt32      |
| long/Long       | UInt64          | ✅         | DataWriter.writeUInt64      |
| BigInteger      | UInt64          | ✅         | DataWriter.writeUInt64      |
| BigInteger      | UInt128         | ✅         | DataWriter.writeUInt128     |
| BigInteger      | UInt256         | ✅         | DataWriter.writeUInt256     |
| BigDecimal      | Decimal         | ✅         | DataWriter.writeDecimal     |
| BigDecimal      | Decimal32       | ✅         | DataWriter.writeDecimal     |
| BigDecimal      | Decimal64       | ✅         | DataWriter.writeDecimal     |
| BigDecimal      | Decimal128      | ✅         | DataWriter.writeDecimal     |
| BigDecimal      | Decimal256      | ✅         | DataWriter.writeDecimal     |
| float/Float     | Float           | ✅         | DataWriter.writeFloat32     |
| double/Double   | Double          | ✅         | DataWriter.writeFloat64     |
| boolean/Boolean | Boolean         | ✅         | DataWriter.writeBoolean     |
| String          | String          | ✅         | DataWriter.writeString      |
| String          | FixedString     | ✅         | DataWriter.writeFixedString |
| LocalDate       | Date            | ✅         | DataWriter.writeDate        |
| LocalDate       | Date32          | ✅         | DataWriter.writeDate32      |
| LocalDateTime   | DateTime        | ✅         | DataWriter.writeDateTime    |
| ZonedDateTime   | DateTime        | ✅         | DataWriter.writeDateTime    |
| LocalDateTime   | DateTime64      | ✅         | DataWriter.writeDateTime64  |
| ZonedDateTime   | DateTime64      | ✅         | DataWriter.writeDateTime64  |
| int/Integer     | Time            | ❌         | N/A                         |
| long/Long       | Time64          | ❌         | N/A                         |
| byte/Byte       | Enum8           | ✅         | DataWriter.writeInt8        |
| int/Integer     | Enum16          | ✅         | DataWriter.writeInt16       |
| java.util.UUID  | UUID            | ✅         | DataWriter.writeIntUUID     |
| String          | JSON            | ✅         | DataWriter.writeJSON        |
| Array<Type>     | Array<Type>     | ✅         | DataWriter.writeArray       |
| Map<K,V>        | Map<K,V>        | ✅         | DataWriter.writeMap         |
| Tuple<Type,..>  | Tuple<T1,T2,..> | ✅         | DataWriter.writeTuple       |
| Object          | Variant         | ❌         | N/A                         |
| (inner type T)  | SimpleAggregateFunction(f, T) | ✅ | writer for inner type T |

* A ZoneId must also be provided when performing date operations. 
* Precision and scale must also be provided when performing decimal operations. 
* To use JSON type as a string, you need to enable `enableJsonSupportAsString` in `ClickHouseClientConfig` . 
* `SimpleAggregateFunction(f, T)` is wire-encoded exactly as its inner type `T`, so bind the Java value for `T` and ignore the aggregate function `f`. Any `T` supported above works, including `Nullable`, `LowCardinality`, `Decimal`, `Array`, `Map` and `Tuple`. 

## Configuration Options

### Client configuration

| Parameters    | Description                  | Default Value |
|---------------|------------------------------|----------|
| url           | fully qualified URL          | N/A         |
| username      | ClickHouse database username | N/A        |
| password      | ClickHouse database password | N/A        |
| database      | ClickHouse database name     | N/A        |
| table         | ClickHouse table name        | N/A        |

### Sink configuration

Our Sink is built on top of Flink’s `AsyncSinkBase`  

| Parameters    | Description                                                                           | Default Value |
|---------------|---------------------------------------------------------------------------------------|----------|
| maxBatchSize           | Maximum number of records inserted in a single batch                                  | N/A         |
| maxInFlightRequests      | The maximum number of in flight requests allowed before the sink applies backpressure | N/A        |
| maxBufferedRequests      | The maximum number of records that may be buffered in the sink before backpressure is applied                                                          | N/A        |
| maxBatchSizeInBytes      | The maximum size (in bytes) a batch may become. All batches sent will be smaller than or equal to this size                                                              | N/A        |
| maxTimeInBufferMS         | The maximum time a record may stay in the sink before being flushed                                                                 | N/A        |
| maxRecordSizeInBytes         | The maximum record size that the sink will accept, records larger than this will be automatically rejected                                                                 | N/A        |

### Sink Metrics

Our Sink exposes additional metrics on top of Flink's existing metrics:

| Metric | Description | Type | Status |
|--------|-------------|------|--------|
| numBytesSend | Total number of bytes sent to ClickHouse | Counter | ✅ |
| numRecordSend | Total number of records sent to ClickHouse | Counter | ✅ |
| numRequestSubmitted | Total number of requests sent (actual number of flushes performed) | Counter | ✅ |
| numOfDroppedBatches | Total number of batches dropped due to non-retryable failures | Counter | ✅ |
| numOfDroppedRecords | Total number of records dropped due to non-retryable failures | Counter | ✅ |
| totalBatchRetries | Total number of batch retries due to retryable failures | Counter | ✅ |
| writeLatencyHistogram | Histogram of write latency distribution | Histogram | ✅ |
| writeFailureLatencyHistogram | Histogram of write failure latency distribution | Histogram | ✅ |
| triggeredByMaxBatchSizeCounter | Sink flushes triggered by reaching `maxBatchSize` | Counter | ✅ |
| triggeredByMaxBatchSizeInBytesCounter | Sink flushes triggered by reaching `maxBatchSizeInBytes` | Counter | ✅ |
| triggeredByMaxTimeInBufferMSCounter | Sink flushes triggered by reaching `maxTimeInBufferMS` | Counter | ✅ |
| actualRecordsPerBatchHistogram | Histogram of actual batch size distribution | Histogram | ✅ |
| actualBytesPerBatchHistogram | Histogram of actual bytes per batch distribution | Histogram | ✅ |
| actualTimeInBufferHistogram | Histogram of actual time in buffer before flush distribution | Histogram | ❌ |

## Limitations

* Currently the sink does not support exactly-once semantics 

## Compatibility

- All projects in this repo are tested with all [active LTS versions](https://github.com/ClickHouse/ClickHouse/pulls?q=is%3Aopen+is%3Apr+label%3Arelease) of ClickHouse.
- [Support policy](https://github.com/ClickHouse/ClickHouse/blob/master/SECURITY.md#security-change-log-and-support)
- We recommend upgrading the connector continuously to not miss security fixes and new improvements
  - If you have an issue with migration - create and issue and we will respond!

## Contributing

Please see our [contributing guide](./CONTRIBUTING.md). 

