name := "Stream Handler"

version := "1.0"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq (
    "org.apache.spark" %% "spark-core" % "3.3.1" % "provided",
    "org.apache.spark" %% "spark-sql" % "3.3.1" % "provided",
    "mysql" % "mysql-connector-java" % "8.0.28"
)