/*
 * Build file of the flink-connector-clickhouse-table submodule.
 *
 * Version-independent Table API / SQL support: connector options, the
 * (Flink LogicalType, ClickHouseColumn) type matrix, schema resolution and the
 * RowData -> Map DataMapper. Compiled once against flink-table-common 1.17
 * (compileOnly — provided by the Flink distribution) and used unchanged on 2.x;
 * only @PublicEvolving API that is stable in practice is referenced.
 * See docs/table-api/dld-build-packaging.md.
 */

plugins {
    `maven-publish`
    java
    signing
    id("com.gradleup.nmcp") version "0.0.8"
}

val sinkVersion: String by rootProject.extra
val clickhouseVersion: String by rootProject.extra

repositories {
    mavenCentral()
}

extra.apply {
    set("log4jVersion", "2.17.2")
}

// Pinned on purpose: the module is compiled once against the oldest supported
// flink-table-common and reused on every Flink generation (hld.md §3).
val flinkTableCommonVersion = "1.17.2"

dependencies {
    api(project(":flink-connector-clickhouse-base"))

    // ClickHouseColumn & co. — the base module does not export them.
    implementation("com.clickhouse:client-v2:${clickhouseVersion}:all")

    // Provided by the Flink distribution at runtime — never bundled.
    compileOnly("org.apache.flink:flink-table-common:$flinkTableCommonVersion")

    testImplementation("org.apache.flink:flink-table-common:$flinkTableCommonVersion")
    testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:${project.extra["log4jVersion"]}")
    testImplementation("org.apache.logging.log4j:log4j-api:${project.extra["log4jVersion"]}")
    testImplementation("org.apache.logging.log4j:log4j-core:${project.extra["log4jVersion"]}")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

sourceSets {
    main {
        java {
            srcDirs("src/main/java")
        }
    }
    test {
        java {
            srcDirs("src/test/java")
        }
    }
}
