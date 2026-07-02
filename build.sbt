ThisBuild / scalaVersion := "2.12.20"

lazy val root = project
  .in(file("."))
  .settings(
    name := "junior-data-exercise",
    version := "1.0",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.8"
    ),

    fork := true,

    javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-exports=java.base/sun.security.action=ALL-UNNAMED"
    )
  )