# map-evolution example (Flink 1.x)

Demonstrates schema evolution under the Map-based payload design: the user's
`DataMapper` declares the columns, the connector checkpoints the `Map<String,
Object>` (not the POJO), and a new release can add columns by changing the
mapper without breaking restore from an older checkpoint.

## Files

- `Event.java` — input POJO. Build A populates `a`, `b`; Build B adds `c`.
- `MapEvolutionMapperA.java` — `DataMapper` for the initial schema.
- `MapEvolutionMapperB.java` — `DataMapper` for the evolved schema (adds `c`).
- `MapEvolutionJob.java` — single job; `-build A` or `-build B` picks the mapper.

## Build

```bash
mvn -q clean package
```

Produces `target/map-evolution-1.0-SNAPSHOT.jar`.

## Run sequence

### 1. Create the initial table

```sql
CREATE TABLE map_evolution (a Int32, b String) ENGINE = MergeTree ORDER BY a;
```

### 2. Run Build A, take a savepoint before flush

```bash
flink run \
  -c com.example.MapEvolutionJob \
  target/map-evolution-1.0-SNAPSHOT.jar \
  -url <chURL> -username <u> -password <p> -database <db> \
  -table map_evolution \
  -build A -records 100 -idStart 0
```

The job's `maxTimeInBufferMS` is 10 minutes so records stay buffered.

```bash
flink savepoint <jobId> file:///tmp/map-evo
flink cancel <jobId>
```

### 3. Evolve the table

```sql
ALTER TABLE map_evolution ADD COLUMN c Nullable(String) DEFAULT 'omg';
```

### 4. Run Build B from the savepoint

```bash
flink run -s <savepoint-path> \
  -c com.example.MapEvolutionJob \
  target/map-evolution-1.0-SNAPSHOT.jar \
  -url <chURL> -username <u> -password <p> -database <db> \
  -table map_evolution \
  -build B -records 100 -idStart 100
```

### 5. Verify

```sql
SELECT count() FROM map_evolution;
-- 200

SELECT * FROM map_evolution WHERE a < 100 LIMIT 3;
-- a=0  b=name-0  c=NULL    (restored Build A entry — no "c" key in the Map; mapper writes null)
-- a=1  b=name-1  c=NULL
-- a=2  b=name-2  c=NULL

SELECT * FROM map_evolution WHERE a >= 100 LIMIT 3;
-- a=100 b=name-100 c=extra-100  (fresh Build B entry — full data)
-- a=101 b=name-101 c=extra-101
-- a=102 b=name-102 c=extra-102
```

## What's happening

- Build A checkpoints `Map{"a"→i, "b"→"name-i"}`.
- On restore under Build B, the Map is read back unchanged. Build B's mapper
  iterates **its** bindings: `a`, `b`, `c`. For restored entries, `c` is missing
  from the Map; `Map.get("c")` returns null; the writer emits a NULL on the wire
  (the binding is `Nullable(String)`, so this is valid).
- Fresh entries emitted by Build B carry an actual value for `c`.
- The table's `DEFAULT 'omg'` clause only fires for omitted-from-wire columns,
  not for explicit nulls on a Nullable column. If you wanted restored entries to
  pick up `'omg'` instead of NULL, change Build B's mapper to use
  `ColumnBinding.scalar("c", "c", ClickHouseDataType.String)` (non-Nullable) and
  the writer will rely on `input_format_null_as_default = 1` (already set) to
  substitute the DEFAULT — but only if you also configure the table column as
  non-Nullable.
