/*
 *  This file is the build file of flink-connector-clickhouse-base submodule
 * 
 */

plugins {
    `maven-publish`
    java
    signing
    id("com.gradleup.nmcp") version "0.0.8"
//    id("com.github.johnrengelman.shadow") version "8.1.1"
}

val scalaVersion = "2.13.12"
val sinkVersion: String by rootProject.extra
val clickhouseVersion: String by rootProject.extra // Temporary until we have a Java Client release

repositories {
    maven("https://central.sonatype.com/repository/maven-snapshots/") // Temporary until we have a Java Client release
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
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    // logger
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:${project.extra["log4jVersion"]}")
    implementation("org.apache.logging.log4j:log4j-api:${project.extra["log4jVersion"]}")
    implementation("org.apache.logging.log4j:log4j-1.2-api:${project.extra["log4jVersion"]}")
    implementation("org.apache.logging.log4j:log4j-core:${project.extra["log4jVersion"]}")

    // ClickHouse Client Libraries
    implementation("com.clickhouse:client-v2:${clickhouseVersion}:all")

    // For testing
    testImplementation(testFixtures(project(":flink-connector-clickhouse-base")))
    testImplementation("org.testcontainers:testcontainers:${project.extra["testContainersVersion"]}")
    testImplementation("com.squareup.okhttp3:okhttp:5.1.0")
    testImplementation("com.google.code.gson:gson:2.10.1")
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
