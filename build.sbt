name := "findhotel"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= {
  Seq(
    "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
    "mysql" % "mysql-connector-java" % "5.1.16",
    "com.databricks" %% "spark-redshift" % "3.0.0-preview1",
    "org.postgresql" % "postgresql" % "42.1.4",
    "org.scalatest" %% "scalatest" % "3.0.4" % "test"
  )
}

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}