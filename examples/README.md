<div align="center">
<p><img src="https://github.com/ClickHouse/clickhouse-js/blob/a332672bfb70d54dfd27ae1f8f5169a6ffeea780/.static/logo.svg" width="200px" align="center"></p>
<h1>ClickHouse Flink Connector</h1>
</div>

Table of Contents
* [Covid Flink Example](#covid-flink-application-example)
* [Build Application](#build-covid-application)
  * [Build Connector](#build-clickhouse-flink-connector)
  * [Java Application](#java-covid-app)
  * [Scala Application](#scala-covid-app)
* [Running Example](#running-the-example)
  * [Download Data](#download-covid-data)
  * [Create table](#create-a-destination-covid-table)
  * [Submit Flink](#submit-flink-job)

# Covid Flink Application example

Read covid data from a file and insert into ClickHouse

### Build Covid Application

#### Build ClickHouse Flink Connector
If you wish to build the connector locally run before building the example
```bash
./gradlew publishToMavenLocal
```

#### Java Covid App

Two parallel Maven projects, one per Flink major version:

- `examples/maven/flink-v1.7/covid` — Flink 1.17/1.18/1.19/1.20 (uses connector artifact `flink-connector-clickhouse-1.17`)
- `examples/maven/flink-v2/covid`  — Flink 2.0+ (uses connector artifact `flink-connector-clickhouse-2.0.0`)

From the chosen project directory, run the following — produces a `covid-1.0-SNAPSHOT.jar` in `target/`:

```bash
mvn clean package -DskipTests
```

#### Scala Covid App

Two parallel sbt projects mirroring the Java layout:

- `examples/sbt/flink-v1.7/covid` — Flink 1.17 (uses connector artifact `flink-connector-clickhouse-1.17`)
- `examples/sbt/flink-v2/covid`  — Flink 2.0 (uses connector artifact `flink-connector-clickhouse-2.0.0`)

From the chosen project directory, run the following — produces a `covid.jar` in `target/scala-2.12/`:

```bash
sbt clean assembly
```

## Running the example

- Prepare ClickHouse OSS or [ClickHouse Cloud](https://clickhouse.com/)
- Flink Cluster or Standalone running
- Download covid data

### Download covid data

Download covid data set and save it in a location that is accessible to Flink

```bash
curl -L -# -o epidemiology.csv https://storage.googleapis.com/covid19-open-data/v3/epidemiology.csv
```

### Create a destination covid table

```sql

CREATE TABLE IF NOT EXISTS `default`.`covid` (
    date Date,
    location_key LowCardinality(String),
    new_confirmed Int32,
    new_deceased Int32,
    new_recovered Int32,
    new_tested Int32,
    cumulative_confirmed Int32,
    cumulative_deceased Int32,
    cumulative_recovered Int32,
    cumulative_tested Int32
) ENGINE = MergeTree
ORDER BY (location_key, date);
```

### Submit Flink Job

With the Java `covid-1.0-SNAPSHOT.jar` or Scala `covid.jar` built, you can now submit the job to your Flink cluster (or standalone instance) 

```bash
# Run the application
./bin/flink run \
  /path/to/your/generated/jar \
  -input "/path/to/epidemiology.csv" \
  -url "/url/clickhouse" \
  -username "default" \
  -password "" \
  -database "default" \
  -table "covid"
```
