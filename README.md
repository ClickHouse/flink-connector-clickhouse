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
    <version>0.1.3</version>
    <classifier>all</classifier>
</dependency>
```

### For Flink 1.17+ 

Maven 

```xml
<dependency>
    <groupId>com.clickhouse.flink</groupId>
    <artifactId>flink-connector-clickhouse-1.17</artifactId>
    <version>0.1.3</version>
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

* A ZoneId must also be provided when performing date operations. 
* Precision and scale must also be provided when performing decimal operations. 
* To use JSON type as a string, you need to enable `enableJsonSupportAsString` in `ClickHouseClientConfig` . 

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

