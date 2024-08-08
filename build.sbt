ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "mysql" % "mysql-connector-java" % "8.0.30",
  "io.delta" %% "delta-spark" % "3.2.0",
  "org.scalatest" %% "scalatest" % "3.2.16" % "test"
)
lazy val root = (project in file("."))
  .settings(
    name := "FlattenSLSJson"
  )
