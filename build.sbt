name := "GraphxTest"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.1"

libraryDependencies ++= Seq(
  // https://mvnrepository.com/artifact/org.apache.spark/spark-core
  "org.apache.spark" %% "spark-core" % sparkVersion,
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // https://mvnrepository.com/artifact/org.apache.spark/spark-graphx
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  // https://mvnrepository.com/artifact/org.apache.spark/spark-hive
  "org.apache.spark" %% "spark-hive" % sparkVersion
)
