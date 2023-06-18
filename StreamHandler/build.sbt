name := "StreamHandler"
version := "1.0"
scalaVersion := "2.12.17"

val sparkVersion = "3.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.2.0",
  "com.datastax.oss" % "java-driver-core" % "4.13.0",
  "com.datastax.cassandra" % "cassandra-driver-core" % "4.0.0",
)

