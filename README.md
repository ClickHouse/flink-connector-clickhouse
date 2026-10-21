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
- Table API / Flink SQL (inserts only, no updates or deletes)

## Supported Flink Versions

| Version | Dependency                       | ClickHouse Client Version | Required Java |
|---------|----------------------------------|---------------------------|---------------|
| latest  | flink-connector-clickhouse-2.0.0 | 0.11.0-rc1                | Java 17+      |
| 2.0.1   | flink-connector-clickhouse-2.0.0 | 0.11.0-rc1                | Java 17+      |
| 2.0.0   | flink-connector-clickhouse-2.0.0 | 0.11.0-rc1                | Java 17+      |
| 1.20.2  | flink-connector-clickhouse-1.17  | 0.11.0-rc1                | Java 11+      |
| 1.19.3  | flink-connector-clickhouse-1.17  | 0.11.0-rc1                | Java 11+      |
| 1.18.1  | flink-connector-clickhouse-1.17  | 0.11.0-rc1                | Java 11+      |
| 1.17.2  | flink-connector-clickhouse-1.17  | 0.11.0-rc1                | Java 11+      |

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

An `INSERT INTO t (a, b)` column list is honoured: the columns the statement leaves out stay
out of the request, so the server applies their `DEFAULT` instead of receiving the planner's
padding `NULL`. This needs Flink 1.18 or newer — Flink 1.17 does not report the column list to
the sink, so a partial insert there still writes an explicit `NULL` into every omitted column.

`MATERIALIZED`, `ALIAS` and `EPHEMERAL` columns take no value from an insert. You may still
declare them in the Flink schema as long as every statement omits them through a column list —
declare them nullable, since Flink refuses to leave a `NOT NULL` column out of one. A statement
that would write such a column fails at planning and names it.

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
| `sink.parallelism` | same as its input (usually the job's default parallelism) | `sinkTo(sink).setParallelism(n)` |

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
| `sink.strict-numeric-mapping` | `false` | `true` rejects at planning every numeric type pair whose values may not all fit the column (a narrower or unsigned integer column, a `DECIMAL(p,0)` at an integer's digit boundary or into an unsigned integer), instead of range-checking each value at write time; lossless widening stays allowed |

**Passthrough**: `clickhouse.client.<key>` options are forwarded to the ClickHouse client,
`clickhouse.server.<key>` become per-query server settings. The connector sends
`print_pretty_type_names = 0` with its schema introspection only, so the type names it reads are
canonical (named `Tuple` columns are otherwise pretty-printed across lines and rejected in the
insert header) — this makes ClickHouse 23.10 the SQL sink's minimum server version, as older
servers reject the unknown setting — and pins `input_format_null_as_default`, `input_format_defaults_for_omitted_fields`
and (for `JSON` columns) `input_format_binary_read_json_as_string` on every insert; a user copy of
any of these settings, in either spelling, is rejected. The client option `async` is pinned per
insert the same way, so it is rejected too.

Flink `STRING` into a ClickHouse `JSON` column works out of the box — the connector enables
the client's JSON-as-string mode automatically exactly when a `JSON` column is mapped.

### Type mapping

Lossless widening is implicit; a pair that would round a value (`DOUBLE` into `Float32`, a narrower
`Decimal` scale or timestamp precision) is rejected at planning. A pair whose values may not all fit the
column (the "Notes" below) is accepted and every value is checked at write time, failing the job naming
the column, value and range; with `'sink.strict-numeric-mapping' = 'true'` the numeric pairs among them are
rejected at planning instead, since a wider column can always take their place. Any other pair fails at
planning naming the column and both types.

| Flink SQL type | ClickHouse column types | Notes |
|---|---|---|
| `BOOLEAN` | `Bool` | |
| `TINYINT` / `SMALLINT` / `INT` / `BIGINT` | any `Int8..Int256` or `UInt8..UInt256` | a narrower or unsigned column is range-checked per record |
| `DECIMAL(p, s)` | a `Decimal(p', s')` it fits; with `s = 0` also any `Int8..Int256` / `UInt8..UInt256` whose digits cover `p` | boundary precisions (`DECIMAL(19, 0)` → `Int64`, `DECIMAL(20, 0)` → `UInt64`) and unsigned targets are range-checked per record |
| `FLOAT` | `Float32`, `Float64` | |
| `DOUBLE` | `Float64` | `Float32` would round — `CAST` to `FLOAT` to round explicitly |
| `CHAR` / `VARCHAR` / `STRING` | `String`, `FixedString(n)`, `UUID`, `JSON` | `FixedString` length checked in UTF-8 bytes per record, whatever the declared Flink length (Flink does not enforce it by default); `UUID` text checked per record |
| `DATE` | `Date`, `Date32` | range-checked per record: `Date` 1970-01-01..2149-06-06, `Date32` 1900-01-01..2299-12-31 |
| `TIMESTAMP(p)` / `TIMESTAMP_LTZ(p)` | `DateTime` (`p = 0`), `DateTime64(s >= p)` | range-checked per record: `DateTime` 1970-01-01..2106-02-07, `DateTime64` 1900-01-01..2299-12-31 (2262-04-11 at scale 9). Flink's default `TIMESTAMP` is precision 6 — declare `TIMESTAMP(3)` for `DateTime64(3)`. `TIMESTAMP` is a wall clock in `sink.timezone`; `TIMESTAMP_LTZ` an instant |
| `ARRAY<t>` | `Array(T)` | only `Array(Nullable(T))` can carry nested NULLs |
| `MAP<k, v>` | `Map(K, V)` | string/integer/decimal keys except `UInt64`; values not `Nullable` |
| `MULTISET<t>` | `Map(T, I)` for any integer `I` | counts become the values; the value type must not be `Nullable`. A count is a non-negative `INT`, so `Int32`/`UInt32` and wider always fit; `Int8`, `Int16`, `UInt8`, `UInt16` are range-checked per record and rejected under `sink.strict-numeric-mapping` |
| `ROW<...>` | `Tuple(...)` | positional — a ROW whose field names are a named Tuple's element names in another order is rejected at planning; fields/elements not nullable |

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

## Delegation tokens (short-lived JWTs for ClickHouse Cloud)

Instead of putting a long-lived password into job code, the sink can authenticate to ClickHouse Cloud with
short-lived JWTs obtained by [Flink's delegation token framework](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/security/security-delegation-token/).
The JobManager holds your identity provider's client id and secret, requests an access token (OAuth2
client-credentials grant), and Flink distributes and re-obtains it on every TaskManager.

Requirements: ClickHouse Cloud **Enterprise**, service **26.4+**, and an identity provider (Entra ID, Okta,
Auth0, Keycloak, …) with a machine-to-machine (client-credentials) app.

1. **Identity provider:** create a client-credentials app. Its access token must be a JWT with a `kid` header,
   RS256 (or ES256/384/512 on 26.8+), and the claims `exp`, `iat`, `iss`, `sub`, `aud` (`aud` a single string),
   plus the ClickHouse roles in a claim (e.g. Entra app roles in `roles`).
2. **Cloud console** (*Settings → Security → JWT authentication*, see
   [JWT authentication setup](https://clickhouse.com/docs/products/cloud/guides/security/cloud-access-management/jwt-authentication-setup)):

   | Field | Value |
   |---|---|
   | Issuer | the token's `iss` (Entra v2: `https://login.microsoftonline.com/<tenant>/v2.0`) |
   | Audience | the token's `aud` |
   | JWKS URL | your identity provider's keys URL (e.g. `…/discovery/v2.0/keys`, `…/.well-known/jwks.json`) |
   | Roles claim | the claim holding roles (e.g. `roles`) |

   The roles must exist on the service. Rights are `permission limit (the default user) ∩ roles/grants`;
   anything beyond the `default` user's rights is silently dropped.
3. **Flink:** copy `flink-connector-clickhouse-token-<version>.jar` into `lib/` on **every** JobManager and
   TaskManager (same version everywhere), and configure:

   ```yaml
   security.delegation.token.provider.clickhouse.oauth.token-endpoint: https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token
   security.delegation.token.provider.clickhouse.oauth.client-id: <client-id>
   security.delegation.token.provider.clickhouse.oauth.client-secret: <secret>
   security.delegation.token.provider.clickhouse.oauth.scope: api://<client-id>/.default   # optional
   security.delegation.token.provider.clickhouse.oauth.audience: <api identifier>          # optional (Auth0-style)
   # security.delegation.token.provider.clickhouse.oauth.allow-insecure-http: true         # optional, see notes
   # security.delegation.token.provider.clickhouse.oauth.connect-timeout: 10s              # optional, default 10s
   # security.delegation.token.provider.clickhouse.oauth.request-timeout: 30s              # optional, default 30s
   # security.delegation.token.provider.clickhouse.oauth.retry-interval: 60s               # optional, default 60s
   security.ssl.internal.enabled: true
   ```
4. **Job code:** `ClickHouseClientConfig.forDelegationToken("https://<service>:8443/", database, table)`.
   No username or password. Plain `http://` is rejected.

Notes:
- Only the JobManager needs the client secret, and only the JobManager talks to the identity provider. TaskManagers
  only receive short-lived JWTs.
- The JobManager log warns if the identity provider's token is missing something ClickHouse Cloud requires.
- The token endpoint must be `https://` because the client secret is sent to it. Plain `http://` is accepted for
  `localhost`/`127.0.0.1`/`[::1]`, or for any host when `oauth.allow-insecure-http: true` is set (for example a
  sidecar that terminates TLS). With that flag set, the JobManager logs a warning at every startup.
  Don't put credentials or a `#fragment` in the endpoint URL.
- `oauth.connect-timeout` bounds the TCP/TLS connect to the identity provider. `oauth.request-timeout` bounds the
  whole token request, including reading the response body.
- If the identity provider has a problem, the JobManager retries the request up to 4 times (1s/2s/4s apart) for
  network errors, timeouts, HTTP 5xx and 429. It doesn't retry other 4xx errors such as `invalid_client`.
  If it still fails, TaskManagers keep the last JWT, and the JobManager tries again every `oauth.retry-interval`
  until the identity provider recovers. No `security.delegation.tokens.renewal.retry.backoff` tuning is needed
  for the ClickHouse provider. During such an outage Flink also renews other providers' tokens (HDFS, S3) on
  that interval.
- Tokens apply to the whole Flink cluster: on a session cluster every job shares the same ClickHouse identity.
- Keep `org.apache.flink.` in `classloader.parent-first-patterns.default` (Flink's default).
- Self-managed / OSS ClickHouse doesn't support JWT; keep using username/password there.
- Delegation tokens are supported by the DataStream sink (`ClickHouseClientConfig.forDelegationToken`).
  Table API / SQL sinks use username/password.

## Limitations

* Currently the sink does not support exactly-once semantics 

## Compatibility

- All projects in this repo are tested with all [active LTS versions](https://github.com/ClickHouse/ClickHouse/pulls?q=is%3Aopen+is%3Apr+label%3Arelease) of ClickHouse.
- The Table API / SQL sink requires ClickHouse 23.10 or newer (see "Passthrough" under [Connector options](#connector-options)); the DataStream API has no such floor.
- [Support policy](https://github.com/ClickHouse/ClickHouse/blob/master/SECURITY.md#security-change-log-and-support)
- We recommend upgrading the connector continuously to not miss security fixes and new improvements
  - If you have an issue with migration - create and issue and we will respond!

## Contributing

Please see our [contributing guide](./CONTRIBUTING.md). 

