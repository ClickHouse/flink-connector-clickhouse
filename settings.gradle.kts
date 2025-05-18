/*
 * settings file for flink-connector-clickhouse a multi module project
 *
 */

plugins {
    // Apply the foojay-resolver plugin to allow automatic download of JDKs
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.10.0"
}

rootProject.name = "flink-connector-clickhouse"
include("flink-connector-clickhouse-base")
