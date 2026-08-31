# Code review findings — feat/42-table-api-sql-sink

Review of `main...HEAD` (~2,900 added lines: Table API/SQL sink). 11 open findings, ordered by priority — original numbering kept. Findings 1–4 (stale planner schema memo, credential-less cache key, planner-side client leak, `sink.max-retries` driving the planning ping) were fixed on this branch (de-memoized `TableIntrospector`; `createPlanningClient()` is closed after introspection and pings a fixed 3 attempts, re-interrupting on cancel) and removed. Where a finding says "identical in the 1.17 copy", apply the fix to both the `flink-connector-clickhouse-2.0.0` and `flink-connector-clickhouse-1.17` variants of the file.

---

## High priority

### 5. Signed→unsigned mappings have no sign/range validation
`flink-connector-clickhouse-table/src/main/java/org/apache/flink/connector/clickhouse/table/schema/ClickHouseTypeMapper.java:283`

The canonical signed→unsigned pairs (SMALLINT→UInt8, INT→UInt16, BIGINT→UInt32, DECIMAL(20,0)→UInt64) are validated neither at planning nor in the converters.

**Failure:** BIGINT `-1` into UInt32 (or DECIMAL(20,0) value `99999999999999999999` into UInt64, which passes the precision-20 planning check since UInt64.MAX is 18446744073709551615) → at flush `BinaryStreamUtils` throws `IllegalArgumentException "long(-1) should be between 0 and 4294967295..."` with no column/row context, and the default `stop-flink` strategy kills the job on one bad row. The same `-1` nested inside an Array/Map/Tuple, or as a MULTISET count, goes through the unchecked `writeUnsignedInt64(long)`/`SerializerUtils` path and is **silently stored** as 18446744073709551615.

**Fix direction:** Range-check in the converters (top-level and nested uniformly) and throw an error naming the column and value, mirroring the existing DATE→Date check.

### 6. Date32/DateTime64 writes skip range checks; DateTime error is column-less
`flink-connector-clickhouse-table/src/main/java/org/apache/flink/connector/clickhouse/table/schema/ClickHouseTypeMapper.java:389`

Only DATE→Date received the PR's friendly range check. DATE→Date32 and TIMESTAMP→DateTime64 write out-of-range values raw; TIMESTAMP→DateTime throws a generic error.

**Failure:** DATE `'9999-12-31'` into Date32 (valid 1900-01-01..2299-12-31): epochDay 2932896 is written as raw Int32 (`SerializerUtils.writeDate32` → `BinaryStreamUtils.writeInt32`, no check) — ClickHouse stores a clamped/wrapped date and SELECT returns a different value than inserted, with no error anywhere. TIMESTAMP `'1969-12-31 23:00:00'` into DateTime → `writeUnsignedInt32` throws `"long(-3600) should be between 0 and 4294967295..."` at flush, killing the batch with no column or row named.

**Fix direction:** Extend the DATE→Date-style range check to Date32, DateTime, and DateTime64 targets.

### 7. UInt64 map keys are re-parsed via `Long.parseLong`
`flink-connector-clickhouse-table/src/main/java/org/apache/flink/connector/clickhouse/table/schema/ClickHouseTypeMapper.java:83`

UInt64 is whitelisted as a string-restorable Map key type, but the client serializer re-parses stringified UInt64 keys with `Long.parseLong`.

**Failure:** `MAP<DECIMAL(20,0) NOT NULL, ...>` mapped to `Map(UInt64, ...)` passes planning; key `18446744073709551615` → `SerializerUtils.convertToLong` → `NumberFormatException` at flush (job death under default `stop-flink`); key `"-1"` parses and is written via unchecked `writeUnsignedInt64(long)` as 18446744073709551615 — silent corruption. Int128/UInt128/Int256/UInt256 keys use BigInteger parsing and are fine.

**Fix direction:** Either route UInt64 keys through the BigInteger path or remove UInt64 from the string-restorable key whitelist (reject at planning).

### 8. PASSWORD option's "masked in logs and EXPLAIN" claim is false
`flink-connector-clickhouse-table/src/main/java/org/apache/flink/connector/clickhouse/table/ClickHouseConnectorOptions.java:41`

No masking exists beyond one constructor log line (which still leaks the password length). EXPLAIN merely omits options, while `SHOW CREATE TABLE` and `COMPILE PLAN`/`EXECUTE PLAN` JSON — which the factory javadoc explicitly advertises — emit the cleartext password verbatim (verified: Flink's `ResolvedCatalogTableJsonSerializer` and `ShowCreateUtil` write options unfiltered).

**Failure:** A user trusts the description, runs `COMPILE PLAN FOR INSERT INTO ch_events ...`, and the plan file contains `'password' = '<cleartext>'`; the file gets checked into VCS, leaking the credential.

**Fix direction:** Correct the option description and README (state plainly that the password appears in catalog/plan serializations), and consider Flink's supported mechanisms for secret options if applicable. Do not claim masking the code doesn't do.

---

## Medium priority

### 9. LogicalTypeRoot exhaustiveness guard test only ever runs against Flink 1.17.2
`flink-connector-clickhouse-table/build.gradle.kts:33`

Version modules share only the `-table` MAIN sources, so `ClickHouseTypeMapperTest` compiles/runs solely against the pinned flink-table-common 1.17.2 floor. It can never fire for the Flink generations where new roots appear — Flink 2.1.0 (a documented cross-compile target in `docs/table-api/cross-compile-changes.md`) already adds DESCRIPTOR and VARIANT.

**Failure:** Builds with `FLINK_2_VERSION=2.1.0` stay green because `buildRules()` is runtime EnumMap registration and the test is not in any version module's test sourceSet; a Flink 2.1 user with a VARIANT/DESCRIPTOR column gets the generic "unknown to this connector build" error and the team never sees a red build.

**Fix direction:** Add the guard test (or a test srcDir) to each version module's test sourceSet so it compiles against the actual Flink version under test.

### 10. New public 9-arg constructor silently hard-codes `enableJsonSupportAsString=false`
`flink-connector-clickhouse-2.0.0/src/main/java/org/apache/flink/connector/clickhouse/sink/ClickHouseClientConfig.java:57` (identical in the 1.17 copy)

The 9-arg (RetryPolicy) constructor contradicts the now-dead field initializer `= true`; correctness depends on the Table API factory remembering a separate `setEnableJsonSupportAsString` call, with nothing enforcing that for other callers.

**Failure:** A DataStream user adopts the public 9-arg constructor and writes Flink STRING values into a ClickHouse JSON column → `input_format_binary_read_json_as_string` is never enabled → inserts fail at first flush with a server-side parse error.

**Fix direction:** Have the 9-arg constructor preserve the documented default (`true`), or remove the dead initializer and make the flag an explicit parameter.

### 11. Table name concatenated unquoted into `DESCRIBE TABLE`
`flink-connector-clickhouse-base/src/main/java/org/apache/flink/connector/clickhouse/introspection/TableIntrospector.java:39`

The raw `table` option is concatenated unquoted into client-v2's `"DESCRIBE TABLE " + table`. A legitimate name needing backquotes fails introspection with the factory's misleading "Could not read the schema" error; raw concatenation is also mechanically injectable (low severity — the option author already holds the credentials).

**Failure:** `WITH ('table' = 'my-table')` for an existing table `` `my-table` `` → server syntax error surfaces as "Could not read the schema of ..." — a naming limitation misreported as connectivity/existence. (The DataStream insert path is likewise unquoted, so such names were already unusable end-to-end; this PR adds the earlier, misleading failure point.)

**Fix direction:** Backquote-escape the identifier (or validate the name at planning with a clear error naming the limitation).

### 12. `copy()` shares the mutable `ClickHouseClientConfig` across planner copies
`flink-connector-clickhouse-2.0.0/src/main/java/org/apache/flink/connector/clickhouse/table/ClickHouseDynamicTableSink.java:86` (identical in the 1.17 copy)

Shares a config with 6 public setters, map mutators, and a transient cached `Client`, violating `DynamicTableSink.copy()`'s documented "deep copy of all mutable members" contract. The justifying comment "All state is planning-time immutable" is factually wrong. Latent today (no current post-construction mutation path), but any future setter use on one copy silently changes every copy, and the shared transient `Client` is reused by all.

**Fix direction:** Deep-copy the config in `copy()` and fix the comment.

---

## Low priority (cleanups)

### 13. SimpleAggregateFunction unwrap logic duplicated across modules
`flink-connector-clickhouse-table/src/main/java/org/apache/flink/connector/clickhouse/table/schema/ClickHouseTypeMapper.java:129`

`unwrapTransparentWrappers` re-implements the unwrap `DataWriter.writeValue` already performs inline (`DataWriter.java:241`) with a differing guard (`hasNestedColumn` vs none). Planning-time acceptance and write-time encoding must agree on which wrappers are transparent; two divergent copies let them drift.

**Fix direction:** Extract one shared helper in the base module and use it from both.

### 14. Batching/backpressure defaults restated as fresh literals
`flink-connector-clickhouse-table/src/main/java/org/apache/flink/connector/clickhouse/table/ClickHouseConnectorOptions.java:59`

The defaults (500, 5mb, 5s, 50, 10000, 1mb) restate `ClickHouseAsyncSinkBuilder`'s private `DEFAULT_*` constants. The javadoc/README promise "defaults are identical to ClickHouseAsyncSinkBuilder", but the values live in 4+ places, so tuning a DataStream default silently de-synchronizes the SQL defaults.

**Fix direction:** Hoist shared public constants into the base module; reference them from both the builder and the options class.

### 15. Integration test byte-identical in both version modules
`flink-connector-clickhouse-2.0.0/src/test/java/org/apache/flink/connector/clickhouse/table/ClickHouseTableApiIntegrationTests.java:1`

The 139-line test is duplicated in the 1.17 and 2.0.0 modules. The repo already has sharing mechanisms (base's testFixtures, or a shared test srcDir mirroring the main-source sharing this PR introduces).

**Fix direction:** Host the test once via a shared test srcDir/testFixtures. (Follows an existing repo convention, so optional for this PR.)

---

## Out of scope for this PR

Pre-existing bug surfaced during the sweep: `ClickHouseAsyncWriter` swallows insert exceptions without completing the `resultHandler`. File as a separate issue; do not fix in this branch.
