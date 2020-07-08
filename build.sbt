
name := "HelloWorld"
version := "0.1.0"
scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.3" % "provided"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.7.3" % "provided"
