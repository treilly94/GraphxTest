name := "GraphxTest"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.1"

libraryDependencies ++= Seq(
  // https://mvnrepository.com/artifact/org.apache.spark/spark-graphx
  "org.apache.spark" %% "spark-graphx" % sparkVersion
)
