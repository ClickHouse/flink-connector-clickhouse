/*
 *  This file is the build file of flink-connector-clickhouse-base submodule
 * 
 */

plugins {
    scala
    java
}

val scalaVersion = "2.13.12"

repositories {
    // Use Maven Central for resolving dependencies.
    mavenLocal()
    mavenCentral()
}

extra.apply {
    set("clickHouseDriverVersion", "0.8.6-SNAPSHOT")
    set("flinkVersion", "2.0.0")
    set("log4jVersion","2.17.2")
    set("testContainersVersion", "1.21.0")
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
    implementation("com.clickhouse:client-v2:${project.extra["clickHouseDriverVersion"]}:all")
    // Apache Flink Libraries
    implementation("org.apache.flink:flink-connector-base:${project.extra["flinkVersion"]}")
    implementation("org.apache.flink:flink-streaming-java:${project.extra["flinkVersion"]}")


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
    testImplementation("org.testcontainers:clickhouse:${project.extra["testContainersVersion"]}")
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
        }
    }
    test {
        scala {
            srcDirs("src/test/scala")
        }
        java {
            srcDirs("src/test/java")
        }
    }
}

// Apply a specific Java toolchain to ease working on different environments.
java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(11)
    }
}

tasks.test {
    useJUnitPlatform()

    include("**/*Test.class", "**/*Tests.class", "**/*Spec.class")
    testLogging {
        events("passed", "failed", "skipped")
        //showStandardStreams = true - , "standardOut", "standardError"
    }
}

tasks.withType<ScalaCompile> {
    scalaCompileOptions.apply {
        encoding = "UTF-8"
        isDeprecation = true
        additionalParameters = listOf("-feature", "-unchecked")
    }
}

tasks.named<Test>("test") {
    // Use JUnit Platform for unit tests.
    useJUnitPlatform()
}

tasks.register<JavaExec>("runScalaTests") {
    group = "verification"
    mainClass.set("org.scalatest.tools.Runner")
    classpath = sourceSets["test"].runtimeClasspath
    args = listOf(
        "-R", "build/classes/scala/test",
        "-oD", // show durations
        "-s", "org.apache.flink.connector.clickhouse.test.scala.ClickHouseSinkTests"
    )
}
