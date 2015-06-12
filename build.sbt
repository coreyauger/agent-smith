import sbt.Keys._

name := "agent-smith"

version := "1.0"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= deps

scalaVersion := "2.11.6"

lazy val deps = {
  val akkaV = "2.3.9"
  val akkaStreamV = "1.0-RC3"
  Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "com.typesafe.akka" %% "akka-stream-experimental" % akkaStreamV,
    "com.typesafe.play" %% "play-json" % "2.3.4",
    "com.typesafe.play" %% "play-ws" % "2.3.4",
    "org.apache.spark" %% "spark-core" % "1.4.0"
  )
}

