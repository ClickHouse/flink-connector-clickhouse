/*
 * Flink delegation token provider/receiver for the ClickHouse sink (OAuth 2.0 client credentials → ClickHouse Cloud JWT).
 * Deployed to Flink's lib/ directory; depends only on flink-core (provided by Flink).
 */

plugins {
    `maven-publish`
    java
    signing
    id("com.gradleup.nmcp") version "0.0.8"
}

val sinkVersion: String by rootProject.extra

// Oldest Flink with the delegation token API (FLIP-272). The interfaces are identical through 2.x.
val flinkTokenApiVersion = "1.17.2"
// Jackson as shaded and shipped by Flink 1.17 in flink-dist (lib/). The package
// org.apache.flink.shaded.jackson2.* is the same in every supported Flink version; never bundled here.
val flinkShadedJacksonVersion = "2.13.4-16.1"

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.apache.flink:flink-core:$flinkTokenApiVersion")
    compileOnly("org.apache.flink:flink-shaded-jackson:$flinkShadedJacksonVersion")

    testImplementation("org.apache.flink:flink-core:$flinkTokenApiVersion")
    testImplementation("org.apache.flink:flink-shaded-jackson:$flinkShadedJacksonVersion")
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<JavaCompile> {
    options.release.set(11)
}

val sourcesJar by tasks.registering(Jar::class) {
    archiveClassifier.set("sources")
    from(sourceSets.main.get().allSource)
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifact(sourcesJar)
            groupId = "com.clickhouse.flink"
            artifactId = "flink-connector-clickhouse-token"
            version = sinkVersion

            pom {
                name.set("ClickHouse Flink Connector - Delegation Tokens")
                description.set("Flink delegation token provider/receiver for the ClickHouse sink (install in Flink lib/)")
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
                }
                scm {
                    connection.set("git@github.com:ClickHouse/flink-connector-clickhouse.git")
                    url.set("https://github.com/ClickHouse/flink-connector-clickhouse")
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
