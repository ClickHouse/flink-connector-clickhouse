

ThisBuild / scalaVersion := "2.12.17"

name := "covid"
organization := "clickhouse"
version := "1.0"

val flinkVersion = "2.0.0"

resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"

libraryDependencies ++= Seq(
  "org.apache.flink" % "flink-streaming-java" % flinkVersion % "provided",
  "org.apache.flink" % "flink-clients" % flinkVersion % "provided",
  "org.apache.flink" % "flink-connector-files" % "2.0.0" % "provided",
  "org.apache.flink.connector" % "clickhouse" % "0.1.2" classifier "all"
)

assembly / assemblyJarName := "covid.jar"

assembly / assemblyExcludedJars := {
  val cp = (assembly / fullClasspath).value
  cp filter { jar =>
    jar.data.getName.contains("flink-") ||
    jar.data.getName.contains("scala-library")
  }
}
