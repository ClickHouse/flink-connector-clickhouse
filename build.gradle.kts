plugins {
    `maven-publish`
    scala
    java
    signing
    id("com.gradleup.nmcp") version "0.0.8"
    id("com.github.johnrengelman.shadow") version "8.1.1"

}

val sinkVersion by extra("0.0.1")
val flinkVersion by extra("1.18.0")
val clickhouseVersion by extra("0.4.6")
val junitVersion by extra("5.8.2")

allprojects {
    group = "org.apache.flink"
    version = "1.0.0"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")

    dependencies {
        testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
    }

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(11))
        }
    }
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
