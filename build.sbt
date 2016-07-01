name := "sparktest2"

version := "1.0"

scalaVersion := "2.10.4"

resolvers ++= Seq(
  "maven Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.5.0-cdh5.5.1" % "provided",
  "org.apache.spark" % "spark-streaming_2.10" % "1.5.0-cdh5.5.1",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.0-cdh5.5.1",
  "org.apache.spark" % "spark-sql_2.10" % "1.5.0-cdh5.5.1",
  "org.apache.spark" % "spark-graphx_2.10" % "1.5.0-cdh5.5.1",
  "mysql" % "mysql-connector-java" % "5.1.34"
)
    