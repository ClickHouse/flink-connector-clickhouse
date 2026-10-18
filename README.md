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
- DataStreamAPI
- Table API (This feature is not implemented yet and is planned for a future release)

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

Table API is planned for a future release. This section will be updated once available.

### Snippet

Planned for a future release — this section will provide a usage snippet for configuring the Table API.

### Example

Planned for a future release — a complete end-to-end example will be added once the Table API becomes available.

## Supported ClickHouse Types

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

## Limitations

* Currently the sink does not support exactly-once semantics 

## Compatibility

- All projects in this repo are tested with all [active LTS versions](https://github.com/ClickHouse/ClickHouse/pulls?q=is%3Aopen+is%3Apr+label%3Arelease) of ClickHouse.
- [Support policy](https://github.com/ClickHouse/ClickHouse/blob/master/SECURITY.md#security-change-log-and-support)
- We recommend upgrading the connector continuously to not miss security fixes and new improvements
  - If you have an issue with migration - create and issue and we will respond!

## Contributing

Please see our [contributing guide](./CONTRIBUTING.md). 

