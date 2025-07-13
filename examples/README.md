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

From project directory run this will create a `covid-1.0-SNAPSHOT.jar` can be found in target folder

Build Covid Java App

```bash
mvn clean package -DskipTests
```

#### Scala Covid App

From project directory run this will create a `covid.jar` can be found in `target/scala-2.12` folder

Build Covid Scala App

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

After you have created java `covid-1.0-SNAPSHOT.jar` or scala `covid.jar` you can submit job to a flink cluster or standalone using `./bin/flink run`

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
