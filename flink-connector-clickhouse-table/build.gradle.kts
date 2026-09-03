/*
 * Build file of the flink-connector-clickhouse-table submodule.
 *
 * Version-independent Table API / SQL support: connector options, the
 * (Flink LogicalType, ClickHouseColumn) type matrix, schema resolution and the
 * RowData -> Map DataMapper.
 *
 * Owns the source but does not ship it: each version module adds src/main/java to its
 * own sourceSet and compiles it against its own Flink. The compile here is the floor
 * check against the oldest supported flink-table-common; the version modules' compile
 * tasks depend on it, so every CI and publish path runs it.
 */

// No publishing/signing plugins: this module ships no artifact (see header).
plugins {
    java
}

val clickhouseVersion: String by rootProject.extra

// Pinned on purpose: this is the floor of the supported range. The version modules
// cross-compile the same source against their own Flink.
val flinkTableCommonVersion = "1.17.2"

dependencies {
    api(project(":flink-connector-clickhouse-base"))

    // ClickHouseColumn & co. — the base module does not export them.
    // Exclude the client's external lz4: the ':all' jar embeds it, and from client 0.10 the
    // at.yawk.lz4 fork capability-clashes with Flink's org.lz4:lz4-java (#160).
    implementation("com.clickhouse:client-v2:${clickhouseVersion}:all") {
        exclude(group = "org.lz4", module = "lz4-java")
        exclude(group = "at.yawk.lz4", module = "lz4-java")
    }

    // Provided by the Flink distribution at runtime — never bundled.
    compileOnly("org.apache.flink:flink-table-common:$flinkTableCommonVersion")

    testImplementation("org.apache.flink:flink-table-common:$flinkTableCommonVersion")
}

// Compile floor check only — the version modules execute these same tests against their own Flink.
tasks.test {
    enabled = false
}
