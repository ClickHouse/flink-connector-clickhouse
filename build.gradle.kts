plugins {
    `maven-publish`
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
}

//sourceSets {
//    main {
//        scala {
//            srcDirs("src/main/scala")
//        }
//        java {
//            srcDirs("src/main/java")
//        }
//    }
//    test {
//        scala {
//            srcDirs("src/test/scala")
//        }
//        java {
//            srcDirs("src/test/java")
//        }
//    }
//}
