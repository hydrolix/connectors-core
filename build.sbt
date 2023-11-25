ThisBuild / organization := "io.hydrolix"
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
Compile / doc / scalacOptions ++= Seq("-no-link-warnings")
Compile / scalacOptions ++= Seq("-deprecation")

Test / fork := true

//noinspection SpellCheckingInspection
libraryDependencies := Seq(
  "com.clickhouse" % "clickhouse-jdbc" % "0.4.6",
  "com.zaxxer" % "HikariCP" % "5.0.1",
  "com.google.guava" % "guava" % "32.0.0-jre",
  "com.lihaoyi" %% "fastparse" % "3.0.2",

  "org.scala-lang.modules" %% "scala-collection-compat" % "2.11.0", // avoid deprecation warnings on JavaConversions
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

credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/hydrolix/connectors-core"),
    "scm:git@github.com:hydrolix/connectors-core.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id = "acruise",
    name = "Alex Cruise",
    email = "alex@hydrolix.io",
    url = url("https://github.com/acruise")
  )
)
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://s01.oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true

releasePublishArtifactsAction := PgpKeys.publishSigned.value
releaseCrossBuild := true