name := "SparkDataReader"

version := "0.1"

scalaVersion := "2.12.12"

val sparkVersion = "2.4.6"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)