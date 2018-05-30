name := "spark-kafka-integration"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq("org.apache.spark" % "spark-sql_2.11" % "2.2.0",
  "org.apache.spark" %% "spark-core" % "2.2.0",
  "com.typesafe" % "config" % "1.2.0",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.0",
  "org.apache.spark" %% "spark-streaming" % "2.2.0")