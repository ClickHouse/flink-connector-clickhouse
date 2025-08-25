<div align="center">
<p><img src="https://github.com/ClickHouse/clickhouse-js/blob/a332672bfb70d54dfd27ae1f8f5169a6ffeea780/.static/logo.svg" width="200px" align="center"></p>
<h1>ClickHouse Flink Connector</h1>
</div>

Table of Contents
* [About The Project](#about-the-project)
* [Supported Flink Versions](#supported-flink-versions)
* [Supported ClickHouse Types](#supported-clickHouse-types)
* [Installation](#installation)
* [DataStream API](#dataStream-api)
    * [Snippets](#snippets)
    * [Examples](#examples)
* [Table API](#table-api)
    * [Artifacts](#artifacts-1)
    * [Examples](#examples-1)
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

## Supported ClickHouse Types

| Java Type     | ClickHouse Type | Supported |
|---------------|-----------------|-----------|
| byte/Byte     | Int8            | ✅         |
| short/Short   | Int16           | ✅         |
| int/Integer   | Int32           | ✅         |
| long/Long     | Int64           | ✅         |
| float/Float   | Float           | ✅         |
| double/Double | Double          | ✅         |

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






### Example

## Table API

### Snippet

### Example

## Compatibility

- All projects in this repo are tested with all [active LTS versions](https://github.com/ClickHouse/ClickHouse/pulls?q=is%3Aopen+is%3Apr+label%3Arelease) of ClickHouse.
- [Support policy](https://github.com/ClickHouse/ClickHouse/blob/master/SECURITY.md#security-change-log-and-support)
- We recommend to upgrade connector continuously to not miss security fixes and new improvements
  - If you have an issue with migration - create and issue and we will respond!

## Contributing

Please see our [contributing guide](./CONTRIBUTING.md). 

