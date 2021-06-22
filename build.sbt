name := "sparkpractice1"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.0-incubating"
