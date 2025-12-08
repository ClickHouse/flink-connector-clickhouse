plugins {
    `maven-publish`
    java
    signing
    id("com.gradleup.nmcp") version "0.0.8"
    id("com.gradleup.shadow") version "9.0.2"
}

val sinkVersion by extra("0.1.2")
val flinkVersion by extra("1.18.0")
val clickhouseVersion by extra("0.9.1")
val junitVersion by extra("5.8.2")

allprojects {
    group = "org.apache.flink"
    version = sinkVersion

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(11))
        }
    }

    dependencies {
        // Use JUnit Jupiter for testing.
//        testImplementation(libs.junit.jupiter)
        testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
        testImplementation("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    }

    tasks.test {
        useJUnitPlatform()

        include("**/*Test.class", "**/*Tests.class", "**/*Spec.class")
        testLogging {
            events("passed", "failed", "skipped")
            //showStandardStreams = true - , "standardOut", "standardError"
        }
    }

    tasks.compileJava {
        options.encoding = "UTF-8"
    }

    tasks.compileTestJava {
        options.encoding = "UTF-8"
    }

    tasks.withType<ScalaCompile> {
        scalaCompileOptions.apply {
            encoding = "UTF-8"
            isDeprecation = true
            additionalParameters = listOf("-feature", "-unchecked")
        }
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
}