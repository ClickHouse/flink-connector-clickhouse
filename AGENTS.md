# Agent Guidelines — Apache Flink ClickHouse Connector

Build, test, and release commands: [`CONTRIBUTING.md`](./CONTRIBUTING.md).
Supported Flink and ClickHouse versions: the CI matrices in
[`.github/workflows/`](./.github/workflows/) — read them, don't guess.

## Testing

Tests need **Docker running** (testcontainers). Every module builds on the root
**Java 11** toolchain (`build.gradle.kts`, `subprojects { java { toolchain … } }`);
CI runs the Gradle launcher on JDK 17.

Beyond the commands in `CONTRIBUTING.md`:

```bash
# Scala tests are a separate task, NOT covered by `test`; both version modules have one
./gradlew clean :flink-connector-clickhouse-1.17:runScalaTests :flink-connector-clickhouse-2.0.0:runScalaTests

# Integration tests: publish both connectors and build the examples FIRST.
# Skipping the example builds fails loudly; skipping the publish does NOT —
# Maven silently resolves the released version from Central and you
# integration-test the wrong connector.
./gradlew :flink-connector-clickhouse-1.17:publishToMavenLocal
./gradlew :flink-connector-clickhouse-2.0.0:publishToMavenLocal
(cd examples/maven/flink-v1.7/covid && mvn -q clean install)
(cd examples/maven/flink-v2/covid   && mvn -q clean install)
./gradlew :flink-connector-clickhouse-integration:test
```

The integration test also has a Scala variant (`integration-tests-scala.yaml`):
`-Dexample.lang=scala` on the same `test` task runs the **sbt** examples instead —
build them first with `sbt clean assembly` in `examples/sbt/{flink-v1.7,flink-v2}/covid`.

Without these, tests only ever run against the default ClickHouse `24.3`:

| Env var | Effect | Default |
|---|---|---|
| `CLICKHOUSE_VERSION` | testcontainers image tag; `cloud` targets ClickHouse Cloud | `24.3` |
| `CLICKHOUSE_CLOUD_HOST` / `CLICKHOUSE_CLOUD_PASSWORD` | **required** when `CLICKHOUSE_VERSION=cloud` | — |
| `FLINK_VERSION` | Flink version for `-1.17`; also picks the example app and cluster image in the integration module | `1.17.2` in `-1.17`, `latest` in the integration tests |

`-2.0.0` has **no** Flink version env var — it hardcodes `2.0.0` in its
`build.gradle.kts`. (`FLINK_2_VERSION` appears in `docs/table-api/` as a proposal,
not as shipped behaviour.)

```bash
CLICKHOUSE_VERSION=25.8 FLINK_VERSION=1.20.2 ./gradlew :flink-connector-clickhouse-1.17:test
```

If Docker or Cloud credentials are unavailable, say so — never skip tests silently
and report success.

### Writing tests

- Read `ClickHouseSinkTestUtils` before adding an end-to-end test and reuse
  `buildSink`/`runJob` — a hand-rolled sink builder passes just as green, and the batch
  tuning constants there are what make the job flush inside the assertion window:
  `runJob` polls for ~10s then **cancels** the job, so a slow-flushing sink loses its
  buffered rows instead of just finishing late.
- Extend `FlinkClusterTests` for anything that needs a server; it owns the
  cluster + container lifecycle in `@BeforeAll`/`@AfterAll`.
- Assert against the server, not the sink: read rows back with
  `ClickHouseServerForTests.extractAllData` and compare to the input. A test that only
  checks the inserted row count proves the sink accepted the data, not that ClickHouse
  stored it correctly.
- Read composite columns with `GenericRecord`'s typed accessors — `getList`, `getTuple`
  — not `toString(col)` in the query. `toString` pins the server's *rendered* form, so
  it passes on a wrong-but-similarly-printed value. `Map` is the exception: no typed
  accessor, use `getObject`.
- Name test classes `*Test`/`*Tests`/`*Spec` — anything else compiles but never runs
  (root `test` include filter). New Scala suites likewise never run until added to
  the `-s` args of `runScalaTests` in the root `build.gradle.kts`.

## Invariants

Breaking these fails silently or at runtime, not in CI:

- `TypeTags` tags, `ClickHouseAsyncSinkSerializer` entry types, and the
  `ClickHousePayload.RAW_KEY` string are **checkpoint wire format**. Never renumber,
  rename, or remove one — old checkpoints must stay readable.
- A new runtime dependency must be added to the `shadowJar` `include` list in **both**
  version modules, or it is missing from the fat jar at runtime while tests still pass.
- `ClickHouseSinkVersion` is **generated** from [`version.txt`](./version.txt) by the
  base module's `generateVersionClass` task. Never edit or commit it.
- Touched a version module — or `-base`, which both embed? Run **both** version
  modules' `test` tasks plus `runScalaTests` — `-1.17` and `-2.0.0` are copy-adapted
  duplicates. Touching `-base` also needs `:flink-connector-clickhouse-base:test`;
  the version modules' test tasks compile it but don't run its tests.
- The embedded ClickHouse test helpers exist in **three** copies: `-base`
  testFixtures (used by the integration module) plus each version module's
  `src/test`. A fix in one copy is silently missing from the others.
- Changing the payload model, wire format, or checkpoint encoding requires a migration
  path in [`CHANGELOG.md`](./CHANGELOG.md).
- Releases are maintainers-only. **Do not bump `version.txt`, tag, or publish.**
