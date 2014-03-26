import AssemblyKeys._

name := """akka-distributed-workers"""

version := "0.1"

scalaVersion := "2.10.3"

resolvers += "maven alt2" at "http://mirrors.ibiblio.org/pub/mirrors/maven2"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-contrib" % "2.3.0",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.0",
  "org.scalatest" % "scalatest_2.10" % "2.0" % "test")


// Assembly settings
mainClass in Global := Some("worker.Main")

jarName in assembly := "worker.jar"

assemblySettings

