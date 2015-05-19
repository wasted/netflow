import scalariform.formatter.preferences._

name := "netflow"

organization := "io.wasted"

version := scala.io.Source.fromFile("version").mkString.trim

scalaVersion := "2.11.6"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-Yinline-warnings", "-Xcheckinit", "-encoding", "utf8", "-feature")

scalacOptions ++= Seq("-language:higherKinds", "-language:postfixOps", "-language:implicitConversions", "-language:reflectiveCalls", "-language:existentials")

javacOptions ++= Seq("-target", "1.7", "-source", "1.7", "-Xlint:deprecation")

mainClass in assembly := Some("io.netflow.Node")

libraryDependencies ++= {
  val wastedVersion = "0.9.5"
  val liftVersion = "2.6.2"
  val phantomVersion = "1.5.0"
  Seq(
    "net.liftweb" %% "lift-json" % liftVersion,
    "io.wasted" %% "wasted-util" % wastedVersion,
    "com.twitter" %% "finagle-redis" % "6.25.0",
    "com.websudos"  %% "phantom-dsl" % phantomVersion,
    "org.xerial.snappy" % "snappy-java" % "1.1.1.3",
    "joda-time" % "joda-time" % "2.7"
  )
}

publishTo := Some("wasted.io/repo" at "http://repo.wasted.io/mvn")

scalariformSettings

ScalariformKeys.preferences := FormattingPreferences().setPreference(AlignParameters, true)

buildInfoSettings

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion) ++ Seq[BuildInfoKey](
  "commit" -> ("git rev-parse HEAD"!!).trim
)

sourceGenerators in Compile <+= buildInfo

buildInfoPackage := "io.netflow.lib"

addArtifact(Artifact("netflow", "server"), assembly)

net.virtualvoid.sbt.graph.Plugin.graphSettings

resolvers ++= Seq(
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
  "wasted.io/repo" at "http://repo.wasted.io/mvn",
  "Websudos releases" at "http://maven.websudos.co.uk/ext-release-local",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Twitter's Repository" at "http://maven.twttr.com/",
  "Typesafe Ivy Repo" at "http://repo.typesafe.com/typesafe/ivy-releases",
  "Java.net Maven2 Repository" at "http://download.java.net/maven/2/"
)

