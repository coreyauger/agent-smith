import sbt.Keys._


name := "agent-smith"

version := "1.0"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "net.sourceforge.htmlcleaner" % "htmlcleaner" % "2.9"

libraryDependencies += "com.typesafe.play" %% "play-json" % "2.3.4"

libraryDependencies += "com.typesafe.play" %% "play-ws" % "2.3.4"
