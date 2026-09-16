# Flink SQL / Table API sink example

A bounded `datagen` source writing into a ClickHouse table declared with
`'connector' = 'clickhouse'`.

The job references no connector class by name — the sink is resolved through the
`org.apache.flink.table.factories.Factory` service file inside the published connector
jar. Running it therefore proves the shaded artifact ships a discoverable Table API
factory, which the in-process tests cannot: they run off the test classpath, so they
would still pass if the service file or the `-table` classes went missing from the jar.

`flink-connector-clickhouse-integration`'s `FlinkTests.testTableApiSqlJob` builds this
example, uploads it to a Flink cluster in Docker, and asserts the row count.

## Build

The connector must be in the local Maven repository first:

```bash
./gradlew :flink-connector-clickhouse-2.0.0:publishToMavenLocal
mvn -q clean package
```

## Run

```bash
flink run -c com.example.SqlSinkJob target/sql-1.0-SNAPSHOT.jar \
  -url http://localhost:8123 \
  -username default \
  -password '' \
  -database default \
  -table sql_sink \
  -records 1000
```

The target table:

```sql
CREATE TABLE sql_sink (
    id     Int64,
    name   Nullable(String),
    amount Nullable(Decimal(10, 2)),
    tags   Array(Nullable(String))
) ENGINE = MergeTree ORDER BY id;
```
