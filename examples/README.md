# Covid Flink Application example

Read covid data from a file and insert into ClickHouse

### Build Covid Application

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

After you have created `covid-1.0-SNAPSHOT.jar` you can submit job to a flink cluster or standalone using `./bin/flink run`

```bash
# Run the application
./bin/flink run \
  /path/to/your/covid-1.0-SNAPSHOT.jar \
  -input "/path/to/epidemiology.csv" \
  -url "/url/clickhouse" \
  -username "default" \
  -password "" \
  -database "default" \
  -table "covid"
```
