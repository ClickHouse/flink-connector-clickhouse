/*
 *  This file is the build file of flink-connector-clickhouse-base submodule
 * 
 */

plugins {
    `maven-publish`
    scala
    java
    signing
    id("com.gradleup.nmcp") version "0.0.8"
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

val scalaVersion = "2.13.12"
val sinkVersion: String by rootProject.extra

repositories {
    // Use Maven Central for resolving dependencies.
    // mavenLocal()
    maven("https://s01.oss.sonatype.org/content/groups/staging/") // Temporary until we have a Java Client release
    mavenCentral()
}

val flinkVersion = System.getenv("FLINK_VERSION") ?: "1.17.2"

extra.apply {
    set("clickHouseDriverVersion", "0.9.0-SNAPSHOT") // Temporary until we have a Java Client release
    set("flinkVersion", flinkVersion)
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
    implementation(project(":flink-connector-clickhouse-base"))

    testImplementation(project(":flink-connector-clickhouse-base"))
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

tasks.jar {
    enabled = false
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            artifact(tasks.shadowJar)
            groupId = "com.clickhouse.flink"
            artifactId = "flink-connector-clickhouse"
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