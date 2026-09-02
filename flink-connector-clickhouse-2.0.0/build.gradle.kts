import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

/*
 * Build configuration for Flink 2.0.0+ ClickHouse Connector
 *
 * This module provides Apache Flink 2.0.0+ compatibility for the ClickHouse connector.
 * It depends on the flink-connector-clickhouse-base module for shared functionality.
 */

plugins {
    `maven-publish`
    scala
    java
    signing
    id("com.gradleup.nmcp") version "0.0.8"
    id("com.gradleup.shadow") version "9.0.2"
}

val scalaVersion = "2.13.12"
val sinkVersion: String by rootProject.extra
val clickhouseVersion: String by rootProject.extra // Temporary until we have a Java Client release

repositories {
    mavenCentral()
}

// Lets CI compile this module against any 2.x. Separate from -1.17's FLINK_VERSION on
// purpose: a shared var would let a 1.x value break this build.
val flinkVersion = System.getenv("FLINK_2_VERSION") ?: "2.0.0"

extra.apply {
    set("flinkVersion", flinkVersion) // the default still will be 2.0.0 since it is more popular currently
    set("log4jVersion","2.17.2")
    set("testContainersVersion", "2.0.2")
    set("testContainersClickHouseVersion", "1.21.3")
    set("byteBuddyVersion", "1.17.5")
}

dependencies {
    // Use JUnit Jupiter for testing.
    testImplementation(libs.junit.jupiter)

    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    implementation("net.bytebuddy:byte-buddy:${project.extra["byteBuddyVersion"]}")
    implementation("net.bytebuddy:byte-buddy-agent:${project.extra["byteBuddyVersion"]}")
    // This dependency is used by the application.
    implementation(libs.guava)
    implementation("org.scala-lang:scala-library:$scalaVersion")
    implementation("org.scala-lang:scala-compiler:$scalaVersion")
    // logger
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:${project.extra["log4jVersion"]}")
    implementation("org.apache.logging.log4j:log4j-api:${project.extra["log4jVersion"]}")
    implementation("org.apache.logging.log4j:log4j-1.2-api:${project.extra["log4jVersion"]}")
    implementation("org.apache.logging.log4j:log4j-core:${project.extra["log4jVersion"]}")

    // ClickHouse Client Libraries
    implementation("com.clickhouse:client-v2:${clickhouseVersion}:all")
    // Apache Flink Libraries
    implementation("org.apache.flink:flink-connector-base:${project.extra["flinkVersion"]}")
    implementation("org.apache.flink:flink-streaming-java:${project.extra["flinkVersion"]}")
    implementation(project(":flink-connector-clickhouse-base"))
    // Table API glue (factory + sink) — provided by the Flink dist, never bundled.
    compileOnly("org.apache.flink:flink-table-common:${project.extra["flinkVersion"]}")

    testImplementation("org.apache.flink:flink-table-common:${project.extra["flinkVersion"]}")
    testImplementation("org.apache.flink:flink-table-api-java-bridge:${project.extra["flinkVersion"]}")
    // planner-loader keeps the planner's Scala 2.12 isolated from this module's Scala 2.13
    testRuntimeOnly("org.apache.flink:flink-table-planner-loader:${project.extra["flinkVersion"]}")
    testRuntimeOnly("org.apache.flink:flink-table-runtime:${project.extra["flinkVersion"]}")
    testImplementation("org.apache.flink:flink-connector-files:${project.extra["flinkVersion"]}")
    testImplementation("org.apache.flink:flink-connector-base:${project.extra["flinkVersion"]}")
    testImplementation("org.apache.flink:flink-streaming-java:${project.extra["flinkVersion"]}")
    testImplementation("org.apache.flink:flink-clients:${project.extra["flinkVersion"]}")
    testImplementation("org.apache.flink:flink-runtime:${project.extra["flinkVersion"]}")
    // logger
    testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:${project.extra["log4jVersion"]}")
    testImplementation("org.apache.logging.log4j:log4j-api:${project.extra["log4jVersion"]}")
    testImplementation("org.apache.logging.log4j:log4j-1.2-api:${project.extra["log4jVersion"]}")
    testImplementation("org.apache.logging.log4j:log4j-core:${project.extra["log4jVersion"]}")
    // flink tests
    testImplementation("org.apache.flink:flink-test-utils:${project.extra["flinkVersion"]}")
    //
    testImplementation("org.testcontainers:testcontainers:${project.extra["testContainersVersion"]}")
    testImplementation("org.testcontainers:clickhouse:${project.extra["testContainersClickHouseVersion"]}")
    testImplementation("org.scalatest:scalatest_2.13:3.2.19")
    testRuntimeOnly("org.scalatestplus:junit-4-13_2.13:3.2.18.0")
//    testRuntimeOnly("org.pegdown:pegdown:1.6.0") // sometimes required by ScalaTest
}

sourceSets {
    main {
        scala {
            srcDirs("src/main/scala")
        }
        java {
            srcDirs("src/main/java")
            // Table API / SQL core, compiled here against this module's Flink rather than consumed as a jar
            srcDir(project(":flink-connector-clickhouse-table").file("src/main/java"))
            srcDir(project(":flink-connector-clickhouse-base").layout.buildDirectory.file("generated/sources/version/java").get().asFile) // to include ClickHouseSinkVersion in the classpath
        }
    }
    test {
        scala {
            srcDirs("src/test/scala")
        }
        java {
            srcDirs("src/test/java")
            // Table API / SQL unit tests, run here against this module's Flink — the
            // LogicalTypeRoot exhaustiveness guard can only fire on the generation under test
            srcDir(project(":flink-connector-clickhouse-table").file("src/test/java"))
            // Shared integration tests; not in -table's own test sourceSet (they need this
            // module's Flink and embedded-ClickHouse test deps)
            srcDir(project(":flink-connector-clickhouse-table").file("src/integrationTest/java"))
        }
    }
}

// These files are hand-duplicated across the version modules (the Table API pair needs this
// module's ClickHouseAsyncSink/ClickHouseClientConfig, so it cannot be single-sourced in
// :flink-connector-clickhouse-table until those are; the config and builder predate the split).
// Fail the build on drift instead. Files that legitimately differ per Flink generation
// (ClickHouseConvertor, ClickHouseAsyncWriter, ...) must stay out of this list.
val checkCrossVersionCopiesInSync by tasks.registering {
    val copies = listOf(
        "src/main/java/org/apache/flink/connector/clickhouse/table/ClickHouseDynamicTableSink.java",
        "src/main/java/org/apache/flink/connector/clickhouse/table/ClickHouseDynamicTableSinkFactory.java",
        "src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory",
        "src/main/java/org/apache/flink/connector/clickhouse/sink/ClickHouseClientConfig.java",
        "src/main/java/org/apache/flink/connector/clickhouse/sink/ClickHouseAsyncSinkBuilder.java",
    ).map { file(it) to project(":flink-connector-clickhouse-1.17").file(it) }
    inputs.files(copies.flatMap { listOf(it.first, it.second) })
    doLast {
        copies.forEach { (ours, theirs) ->
            check(ours.readBytes().contentEquals(theirs.readBytes())) {
                "$ours differs from $theirs — these copies must stay identical; apply the change to both."
            }
        }
    }
}
// check alone never runs in the pipelines: CI invokes test / runScalaTests directly, and
// the publish workflows invoke shadowJar (publishToMavenLocal / CentralPortal build it).
listOf("check", "test", "runScalaTests", "shadowJar").forEach {
    tasks.named(it) { dependsOn(checkCrossVersionCopiesInSync) }
}

tasks.named<ShadowJar>("shadowJar") {
    archiveClassifier.set("all")
    dependencies {
        include(dependency("org.apache.flink.connector.clickhouse:.*"))
        include(project(":flink-connector-clickhouse-base"))
        // :flink-connector-clickhouse-table is absent by design — this filters the runtimeClasspath,
        // and its classes are in this module's own output (see sourceSets).
        include(dependency("com.clickhouse:client-v2:${clickhouseVersion}:all"))
    }
    mergeServiceFiles()
}

val shadowSourcesJar by tasks.registering(Jar::class) {
    archiveClassifier.set("all-sources")
    from(sourceSets.main.get().allSource)
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    dependsOn(":flink-connector-clickhouse-base:generateVersionClass")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            artifact(tasks.shadowJar)
            groupId = "com.clickhouse.flink"
            artifactId = "flink-connector-clickhouse-2.0.0"
            version = sinkVersion

            artifact(shadowSourcesJar)

            pom {
                name.set("ClickHouse Flink Connector")
                description.set("Official Apache Flink connector for ClickHouse")
                url.set("https://github.com/ClickHouse/flink-connector-clickhouse")

                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("https://github.com/ClickHouse/flink-connector-clickhouse/blob/main/LICENSE")
                    }
                }

                developers {
                    developer {
                        id.set("mzitnik")
                        name.set("Mark Zitnik")
                        email.set("mark@clickhouse.com")
                    }
                    developer {
                        id.set("BentsiLeviav")
                        name.set("Bentsi Leviav")
                        email.set("bentsi.leviav@clickhouse.com")
                    }
                }

                scm {
                    connection.set("git@github.com:ClickHouse/flink-connector-clickhouse.git")
                    url.set("https://github.com/ClickHouse/flink-connector-clickhouse")
                }

                organization {
                    name.set("ClickHouse")
                    url.set("https://clickhouse.com")
                }

                issueManagement {
                    system.set("GitHub Issues")
                    url.set("https://github.com/ClickHouse/flink-connector-clickhouse/issues")
                }
            }
        }
    }
}

signing {
    val signingKey = System.getenv("SIGNING_KEY")
    val signingPassword = System.getenv("SIGNING_PASSWORD")
    if (signingKey != null && signingPassword != null) {
        useInMemoryPgpKeys(signingKey, signingPassword)
        sign(publishing.publications["maven"])
    }
}

nmcp {
    publish("maven") {
        username = System.getenv("NMCP_USERNAME")
        password = System.getenv("NMCP_PASSWORD")
        publicationType = "USER_MANAGED"
    }
}