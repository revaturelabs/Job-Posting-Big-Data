import Dependencies._

ThisBuild / scalaVersion     := "2.11.12"
ThisBuild / version          := ""
ThisBuild / organization     := "com.revature"
ThisBuild / organizationName := "revature"

lazy val root = (project in file("."))
  .settings(
    // Change the name for clarity's sake, depending on what you want to/plan on running
    name := "q5keywords-2018-34-agg",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7" % "provided",
    // https://mvnrepository.com/artifact/org.apache.httpcomponents/httpclient
    libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.12",
    // https://mvnrepository.com/artifact/commons-io/commons-io
    libraryDependencies += "commons-io" % "commons-io" % "2.8.0",
    libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.7",
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.7",
    libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.7"
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}