import Dependencies._

ThisBuild / scalaVersion     := "2.13.4"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.Project0"
ThisBuild / organizationName := "Project0"

lazy val root = (project in file("."))
  .settings(
    name := "Project0",
    libraryDependencies += scalaTest % Test,
    // https://mvnrepository.com/artifact/org.postgresql/postgresql
    libraryDependencies += "org.postgresql" % "postgresql" % "42.2.18"

  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
