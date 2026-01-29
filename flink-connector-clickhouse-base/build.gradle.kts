/*
 *  This file is the build file of flink-connector-clickhouse-base submodule
 * 
 */

plugins {
    `maven-publish`
    java
    signing
    `java-test-fixtures`
    id("com.gradleup.nmcp") version "0.0.8"
}

val scalaVersion = "2.13.12"
val sinkVersion: String by rootProject.extra
val clickhouseVersion: String by rootProject.extra // Temporary until we have a Java Client release
val versionFile = "version.txt"

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
}

extra.apply {
    set("log4jVersion","2.17.2")
    set("testContainersVersion", "1.21.0")
    set("byteBuddyVersion", "1.17.5")
}

dependencies {
    // Use JUnit Jupiter for testing.
    testFixturesImplementation(libs.junit.jupiter)
    testFixturesImplementation("org.junit.platform:junit-platform-launcher")

    // logger
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:${project.extra["log4jVersion"]}")
    implementation("org.apache.logging.log4j:log4j-api:${project.extra["log4jVersion"]}")
    implementation("org.apache.logging.log4j:log4j-1.2-api:${project.extra["log4jVersion"]}")
    implementation("org.apache.logging.log4j:log4j-core:${project.extra["log4jVersion"]}")

    // ClickHouse Client Libraries
    implementation("com.clickhouse:client-v2:${clickhouseVersion}:all")

    // For testing
    testFixturesImplementation("com.clickhouse:client-v2:${clickhouseVersion}:all")
    testFixturesImplementation("org.testcontainers:testcontainers:${project.extra["testContainersVersion"]}")
    testFixturesImplementation("org.testcontainers:clickhouse:${project.extra["testContainersVersion"]}")
    testFixturesImplementation("com.squareup.okhttp3:okhttp:5.1.0")
    testFixturesImplementation("com.google.code.gson:gson:2.10.1")
    testFixturesImplementation("org.scalatest:scalatest_2.13:3.2.19")
    testFixturesImplementation("org.scalatestplus:junit-4-13_2.13:3.2.18.0")

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

tasks.register("generateVersionClass") {
    val versionFile = rootProject.file(versionFile)
    val outputDir = project.layout.buildDirectory.file("generated/sources/version/java").get().asFile

    inputs.file(versionFile)
    outputs.dir(outputDir)

    doLast {
        val version = versionFile.readText().trim()
        val versionClass =
"""package org.apache.flink.connector.clickhouse.sink;

public class ClickHouseSinkVersion {
    public static String getVersion() {
        return "$version";
    }
}
"""
        val outputFile = File(outputDir, "org/apache/flink/connector/clickhouse/sink/ClickHouseSinkVersion.java")
        outputFile.parentFile.mkdirs()
        outputFile.writeText(versionClass)
    }
}

tasks.named("compileJava") {
    dependsOn("generateVersionClass")
}
