name := "deduplicate"

version := "1.0"

scalaVersion := "2.11.12"

val spark = "org.apache.spark"
val sparkVersion = "2.2.0"

resolvers ++= Seq(
  "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
)

libraryDependencies ++= Seq(
  "org.scalaz" %% "scalaz-core" % "7.2.18",

  spark %% "spark-core" % sparkVersion,
  spark %% "spark-streaming" % sparkVersion,
  spark %% "spark-sql" % sparkVersion,
  spark %% "spark-graphx" % sparkVersion,
  "graphframes" % "graphframes" % "0.5.0-spark2.1-s_2.11"
)
