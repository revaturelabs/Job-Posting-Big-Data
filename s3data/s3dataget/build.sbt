import Dependencies._

ThisBuild / scalaVersion     := "2.11.12"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.revature"
ThisBuild / organizationName := "revature"

lazy val root = (project in file("."))
  .settings(
    name := "s3dataget",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7" % "provided",
    libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.7",
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.7",
    libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.7"
  )

  assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
