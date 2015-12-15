import sbt.Keys._

name := "agent-smith"

version := "1.0"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= deps

scalaVersion := "2.11.6"

fork := true

lazy val deps = {
  val akkaV = "2.3.9"
  val akkaStreamV = "1.0-RC3"
  Seq(
    "com.typesafe.akka"       %% "akka-actor"                % akkaV,
    "com.typesafe.akka"       %% "akka-stream-experimental"  % akkaStreamV,
    "com.typesafe.play"       %% "play-json"                 % "2.3.4",
    "com.typesafe.play"       %% "play-ws"                   % "2.3.4",
    "org.apache.spark"        %% "spark-core"                % "1.5.2",
    "org.apache.tinkerpop"     %  "gremlin-driver"           % "3.0.1-incubating",
    "org.scala-lang.modules"  %% "scala-java8-compat"        % "0.7.0"
  )
}

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
)
