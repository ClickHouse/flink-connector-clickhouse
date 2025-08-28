<div align="center">
<p><img src="https://github.com/ClickHouse/clickhouse-js/blob/a332672bfb70d54dfd27ae1f8f5169a6ffeea780/.static/logo.svg" width="200px" align="center"></p>
<h1>ClickHouse Flink Connector</h1>
</div>

Table of Contents
* [About The Project](#about-the-project)
* [Supported Flink Versions](#supported-flink-versions)
* [Installation](#installation)
* [DataStream API](#dataStream-api)
    * [Snippets](#snippets)
    * [Examples](#examples)
* [Table API](#table-api)
    * [Artifacts](#artifacts-1)
    * [Examples](#examples-1)
* [Supported ClickHouse Types](#supported-clickHouse-types)
* [Configuration Options](#configuration-options)
  * [Client Configuration](#client-configuration)
  * [Sink Configuration](#sink-configuration)
* [Limitations](#limitations)
* [Contributing](#contributing)

## About The Project

This is a repo of ClickHouse official Apache Flink Connector supported by the ClickHouse team.
The Connector supports to main Apache Flink API's 
- DataStreamAPI
- Table API

## Supported Flink Versions

| Version | Dependency                       | ClickHouse Client Version |
|---------|----------------------------------|---------------------------|
| latest  | flink-connector-clickhouse-2.0.0 | 0.9.1                     |
| 2.0.0   | flink-connector-clickhouse-2.0.0 | 0.9.1                     |
| 1.20.2  | flink-connector-clickhouse-1.17  | 0.9.1                     |
| 1.19.3  | flink-connector-clickhouse-1.17  | 0.9.1                     |
| 1.18.1  | flink-connector-clickhouse-1.17  | 0.9.1                     |
| 1.17.2  | flink-connector-clickhouse-1.17  | 0.9.1                     |

## Installation

### For Flink 2.0.0+

Maven 

```xml
<dependency>
    <groupId>com.clickhouse.flink</groupId>
    <artifactId>flink-connector-clickhouse-2.0.0</artifactId>
    <version>0.0.1</version>
    <type>pom</type>
</dependency>
```

### For Flink 1.17+ 

Maven 

```xml
<dependency>
    <groupId>com.clickhouse.flink</groupId>
    <artifactId>flink-connector-clickhouse-1.17</artifactId>
    <version>0.0.1</version>
    <type>pom</type>
</dependency>
```

## DataStream API

### Snippet

Configure ClickHouseClient 

```java
ClickHouseClientConfig clickHouseClientConfig = new ClickHouseClientConfig(url, username, password, database, tableName);
```
If you are planning to insert RAW CSV data as is 

Create ElementConverter 

```java
ElementConverter<String, ClickHousePayload> convertorString = new ClickHouseConvertor<>(String.class);
```

Create Sink it is important to set format using `setClickHouseFormat`  

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

And after that just wire your DataStream with Sink

```java
data.sinkTo(csvSink);
```

More examples and snippets can be found in our tests [flink-connector-clickhouse-1.17](flink-connector-clickhouse-1.17/src/test/java/org/apache/flink/connector/clickhouse/sink) and [flink-connector-clickhouse-2.0.0](flink-connector-clickhouse-2.0.0/src/test/java/org/apache/flink/connector/clickhouse/sink) 

### Example

## Table API

### Snippet

### Example

## Supported ClickHouse Types

| Java Type       | ClickHouse Type | Supported | Serialize Method       |
|-----------------|-----------------|-----------|------------------------| 
| byte/Byte       | Int8            | ✅         | Serialize.writeInt8    |
| short/Short     | Int16           | ✅         | Serialize.writeInt16   |
| int/Integer     | Int32           | ✅         | Serialize.writeInt32   |
| long/Long       | Int64           | ✅         | Serialize.writeInt64   |
| float/Float     | Float           | ✅         | Serialize.writeFloat32 |
| double/Double   | Double          | ✅         | Serialize.writeFloat64 |
| boolean/Boolean | Boolean         | ✅         | Serialize.writeBoolean |
| String          | String          | ✅         | Serialize.writeString  |

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

Our Sink is build on top Flink `AsyncSinkBase`  

| Parameters    | Description                                                                           | Default Value |
|---------------|---------------------------------------------------------------------------------------|----------|
| maxBatchSize           | Maximum number of records inserted in a single batch                                  | N/A         |
| maxInFlightRequests      | The maximum number of in flight requests allowed before the sink applies backpressure | N/A        |
| maxBufferedRequests      | The maximum number of records that may be buffered in the sink before backpressure is applied                                                          | N/A        |
| maxBatchSizeInBytes      | The maximum size (in bytes) a batch may become. All batches sent will be smaller than or equal to this size                                                              | N/A        |
| maxTimeInBufferMS         | The maximum time a record may stay in the sink before being flushed                                                                 | N/A        |
| maxRecordSizeInBytes         | The maximum record size that the sink will accept, records larger than this will be automatically rejected                                                                 | N/A        |

## Limitations 

* Currently the sink does not support exactly-once semantics 


## Compatibility

- All projects in this repo are tested with all [active LTS versions](https://github.com/ClickHouse/ClickHouse/pulls?q=is%3Aopen+is%3Apr+label%3Arelease) of ClickHouse.
- [Support policy](https://github.com/ClickHouse/ClickHouse/blob/master/SECURITY.md#security-change-log-and-support)
- We recommend to upgrade connector continuously to not miss security fixes and new improvements
  - If you have an issue with migration - create and issue and we will respond!

## Contributing

Please see our [contributing guide](./CONTRIBUTING.md). 

