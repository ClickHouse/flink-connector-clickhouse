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
| latest  | flink-connector-clickhouse-2.0.0 | 0.9.1                     | Java 17+      |
| 2.0.1   | flink-connector-clickhouse-2.0.0 | 0.9.1                     | Java 17+      |
| 2.0.0   | flink-connector-clickhouse-2.0.0 | 0.9.1                     | Java 17+      |
| 1.20.2  | flink-connector-clickhouse-1.17  | 0.9.1                     | Java 11+      |
| 1.19.3  | flink-connector-clickhouse-1.17  | 0.9.1                     | Java 11+      |
| 1.18.1  | flink-connector-clickhouse-1.17  | 0.9.1                     | Java 11+      |
| 1.17.2  | flink-connector-clickhouse-1.17  | 0.9.1                     | Java 11+      |

## Installation

### For Flink 2.0.0+

Maven 

```xml
<dependency>
    <groupId>com.clickhouse.flink</groupId>
    <artifactId>flink-connector-clickhouse-2.0.0</artifactId>
    <version>0.1.2</version>
    <classifier>all</classifier>
</dependency>
```

### For Flink 1.17+ 

Maven 

```xml
<dependency>
    <groupId>com.clickhouse.flink</groupId>
    <artifactId>flink-connector-clickhouse-1.17</artifactId>
    <version>0.1.2</version>
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

Create an ElementConverter 

```java
ElementConverter<String, ClickHousePayload> convertorString = new ClickHouseConvertor<>(String.class);
```

Create the sink and set the format using `setClickHouseFormat`  

```java
ClickHouseAsyncSink<String> csvSink = new ClickHouseAsyncSink<>(
				convertorString,
				MAX_BATCH_SIZE,
				MAX_IN_FLIGHT_REQUESTS,
				MAX_BUFFERED_REQUESTS,
				MAX_BATCH_SIZE_IN_BYTES,
				MAX_TIME_IN_BUFFER_MS,
				MAX_RECORD_SIZE_IN_BYTES,
				clickHouseClientConfig
		);

csvSink.setClickHouseFormat(ClickHouseFormat.CSV);
```

Finally, connect your DataStream to the sink.

```java
data.sinkTo(csvSink);
```

More examples and snippets can be found in our tests [flink-connector-clickhouse-1.17](flink-connector-clickhouse-1.17/src/test/java/org/apache/flink/connector/clickhouse/sink) and [flink-connector-clickhouse-2.0.0](flink-connector-clickhouse-2.0.0/src/test/java/org/apache/flink/connector/clickhouse/sink) 

### Example

We have created maven based example for easy start with ClickHouse Sink 
Different versions for Flink 

- [Flink 1.17+](examples/maven/flink-v1.7/covid)
- [Flink 2.0.0+](examples/maven/flink-v2/covid)

For more detailed instructions, see the [Example Guide](examples#readme)

## Table API

Table API is planned for a future release. This section will be updated once available.

### Snippet

Planned for a future release — this section will provide a usage snippet for configuring the Table API.

### Example

Planned for a future release — a complete end-to-end example will be added once the Table API becomes available.

## Supported ClickHouse Types

| Java Type       | ClickHouse Type | Supported | Serialize Method           |
|-----------------|-----------------|-----------|----------------------------| 
| byte/Byte       | Int8            | ✅         | Serialize.writeInt8        |
| short/Short     | Int16           | ✅         | Serialize.writeInt16       |
| int/Integer     | Int32           | ✅         | Serialize.writeInt32       |
| long/Long       | Int64           | ✅         | Serialize.writeInt64       |
| BigInteger      | Int128          | ✅         | Serialize.writeInt124      |
| BigInteger      | Int256          | ✅         | Serialize.writeInt256      |
| int/Integer     | UInt8           | ✅         | Serialize.writeUInt8       |
| int/Integer     | UInt16          | ✅         | Serialize.writeUInt16      |
| long/Long       | UInt32          | ✅         | Serialize.writeUInt32      |
| long/Long       | UInt64          | ✅         | Serialize.writeUInt64      |
| BigInteger      | UInt128         | ✅         | Serialize.writeUInt128     |
| BigInteger      | UInt256         | ✅         | Serialize.writeUInt256     |
| BigDecimal      | Decimal         | ✅         | Serialize.writeDecimal     |
| BigDecimal      | Decimal32       | ✅         | Serialize.writeDecimal     |
| BigDecimal      | Decimal64       | ✅         | Serialize.writeDecimal     |
| BigDecimal      | Decimal128      | ✅         | Serialize.writeDecimal     |
| BigDecimal      | Decimal256      | ✅         | Serialize.writeDecimal     |
| float/Float     | Float           | ✅         | Serialize.writeFloat32     |
| double/Double   | Double          | ✅         | Serialize.writeFloat64     |
| boolean/Boolean | Boolean         | ✅         | Serialize.writeBoolean     |
| String          | String          | ✅         | Serialize.writeString      |
| String          | FixedString     | ✅         | Serialize.writeFixedString |
| LocalDate       | Date            | ✅         | Serialize.writeDate        |
| LocalDate       | Date32          | ✅         | Serialize.writeDate32      |
| LocalDateTime   | DateTime        | ✅         | Serialize.writeDateTime    |
| LocalDateTime   | DateTime64      | ✅         | Serialize.writeDateTime64  |
| int/Integer     | Time            | ❌         | N/A                        |
| long/Long       | Time64          | ❌         | N/A                        |
| byte/Byte       | Enum8           | ✅         | Serialize.writeInt8        |
| int/Integer     | Enum16          | ✅         | Serialize.writeInt16       |
| java.util.UUID  | UUID            | ✅         | Serialize.writeIntUUID     |
| String          | JSON            | ❌         | N/A                        |
| Array<Type>     | Array<Type>     | ✅         | Serialize.writeArray       |
| Map<K,V>        | Map<K,V>        | ✅         | Serialize.writeMap         |
| Tuple<Type,..>  | Tuple<T1,T2,..> | ✅         | Serialize.writeTuple       |
| Object          | Variant         | ❌         | N/A                        |

* A ZoneId must also be provided when performing date operations. 
* Precision and scale must also be provided when performing decimal operations. 

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

