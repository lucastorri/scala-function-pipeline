organization := "co.torri"

name := "scalaz-stream-test"

version := "0.6.3"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "org.scalaz"     %% "scalaz-core" % "7.0.3",
  "org.scalaz.stream" %% "scalaz-stream" % "0.1-SNAPSHOT",
  "com.typesafe.akka" %% "akka-actor" % "2.2.1"
)

scalacOptions += "-deprecation"

resolvers ++= Seq(
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)
