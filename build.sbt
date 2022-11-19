val scala3Version = "2.13.8"

lazy val root = project
  .in(file("."))
  .settings(
    name := "practice",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies += "org.scalameta" %% "munit" % "0.7.29" % Test,

    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1",

    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1",

    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.1",

    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.1"
  )
