name := "movies_ratings"

version := "1.0"

scalaVersion := "2.13.10"

val sparkVersion = "3.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.holdenkarau" %% "spark-testing-base" % "3.3.1_1.3.0" % "test",
  "org.apache.spark" %% "spark-hive" % sparkVersion
)