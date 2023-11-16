ThisBuild / organization := "io.hydrolix"
ThisBuild / version := "1.0.0-SNAPSHOT"
ThisBuild / organizationHomepage := Some(url("https://hydrolix.io/"))
ThisBuild / homepage := Some(url("https://github.com/hydrolix/connectors-core/"))
ThisBuild / licenses := List(
  "Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"),
  "Proprietary" -> new URL("https://github.com/hydrolix/connectors-core/#proprietary"),
)

scalaVersion := "2.13.12"

crossScalaVersions := List("2.12.18", "2.13.12")

name := "hydrolix-connectors-core"

javacOptions := Seq("-source", "11", "-target", "11")

Test / fork := true

//noinspection SpellCheckingInspection
libraryDependencies := Seq(
  "com.clickhouse" % "clickhouse-jdbc" % "0.4.6",
  "com.zaxxer" % "HikariCP" % "5.0.1",
  "com.google.guava" % "guava" % "32.0.0-jre",
  "com.lihaoyi" %% "fastparse" % "3.0.2",

  "com.github.bigwheel" %% "util-backports" % "2.1",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.3",
  "com.fasterxml.jackson.module" % "jackson-module-parameter-names" % "2.15.3",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.15.3",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % "2.15.3",
  "com.github.ben-manes.caffeine" % "caffeine" % "3.1.5",
  "org.apache.httpcomponents.client5" % "httpclient5" % "5.2.1",
  "ch.qos.logback" % "logback-classic" % "1.4.7",
  "net.java.dev.jna" % "jna" % "5.13.0", // for Wyhash

  "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
  "com.h2database" % "h2" % "2.2.224" % Test,
)
