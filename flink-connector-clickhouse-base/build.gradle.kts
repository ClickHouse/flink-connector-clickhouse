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
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

val scalaVersion = "2.13.12"
val sinkVersion: String by rootProject.extra
val clickhouseVersion: String by rootProject.extra // Temporary until we have a Java Client release

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

// Apply a specific Java toolchain to ease working on different environments.
//java {
//    toolchain {
//        languageVersion = JavaLanguageVersion.of(11)
//    }
//}

//tasks.test {
//    useJUnitPlatform()
//
//    include("**/*Test.class", "**/*Tests.class", "**/*Spec.class")
//    testLogging {
//        events("passed", "failed", "skipped")
//        //showStandardStreams = true - , "standardOut", "standardError"
//    }
//}
//
//tasks.withType<ScalaCompile> {
//    scalaCompileOptions.apply {
//        encoding = "UTF-8"
//        isDeprecation = true
//        additionalParameters = listOf("-feature", "-unchecked")
//    }
//}
//
//tasks.named<Test>("test") {
//    // Use JUnit Platform for unit tests.
//    useJUnitPlatform()
//}
//
//tasks.register<JavaExec>("runScalaTests") {
//    group = "verification"
//    mainClass.set("org.scalatest.tools.Runner")
//    classpath = sourceSets["test"].runtimeClasspath
//    args = listOf(
//        "-R", "build/classes/scala/test",
//        "-oD", // show durations
//        "-s", "org.apache.flink.connector.clickhouse.test.scala.ClickHouseSinkTests"
//    )
//}
//
tasks.shadowJar {
    archiveClassifier.set("all")

    dependencies {
        exclude(dependency("org.apache.flink:.*"))
    }
    mergeServiceFiles()
}

val shadowSourcesJar by tasks.registering(Jar::class) {
    archiveClassifier.set("all-sources")
    from(sourceSets.main.get().allSource)
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

//tasks.jar {
//    enabled = false
//}

publishing {
    publications {
        create<MavenPublication>("maven") {
            artifact(tasks.shadowJar)
            groupId = "com.clickhouse.flink"
            artifactId = "flink-connector-clickhouse-base"
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
        publicationType = "AUTOMATIC"
    }
}