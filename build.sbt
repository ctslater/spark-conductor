name := "spark-conductor"

version := "1.0"

scalaVersion := "2.11.8"

//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.1"

libraryDependencies += "com.typesafe.akka" %% "akka-http" % "10.0.9"
libraryDependencies += "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.9"
libraryDependencies += "com.typesafe.akka" %% "akka-http-testkit" % "10.0.9" % Test
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test"
  


