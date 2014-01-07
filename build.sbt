import AssemblyKeys._
import scalariform.formatter.preferences._

name := "netflow"

organization := "io.wasted"

version := scala.io.Source.fromFile("version").mkString.trim

scalaVersion := "2.10.2"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-Yinline-warnings", "-Xcheckinit", "-encoding", "utf8", "-feature")

scalacOptions ++= Seq("-language:higherKinds", "-language:postfixOps", "-language:implicitConversions", "-language:reflectiveCalls", "-language:existentials")

javacOptions ++= Seq("-target", "1.7", "-source", "1.7", "-Xlint:deprecation")

mainClass in assembly := Some("io.netflow.Node")

jarName in assembly := "netflow-" + scala.io.Source.fromFile("version").mkString.trim + ".jar"

resolvers ++= Seq(
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
  "wasted.io/repo" at "http://repo.wasted.io/mvn",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Twitter's Repository" at "http://maven.twttr.com/",
  "Kungfuters" at "http://maven.kungfuters.org/content/groups/public/",
  "Typesafe Ivy Repo" at "http://repo.typesafe.com/typesafe/ivy-releases",
  "Java.net Maven2 Repository" at "http://download.java.net/maven/2/"
)

libraryDependencies ++= {
  val astyanaxVersion = "1.56.44"
  Seq(
    "io.wasted" %% "wasted-util" % "0.7.6",
    "joda-time" % "joda-time" % "2.3",
    "org.joda" % "joda-convert" % "1.4",
    "com.lambdaworks" % "lettuce" % "2.3.0",
    "com.datastax.cassandra" % "cassandra-driver-core" % "2.0.0-rc2",
    "org.specs2" %% "specs2" % "2.3.6" % "test"
  )
}

publishTo := Some("wasted.io/repo" at "http://repo.wasted.io/mvn")

scalariformSettings

ScalariformKeys.preferences := FormattingPreferences().setPreference(AlignParameters, true)

assemblySettings

buildInfoSettings

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion) ++ Seq[BuildInfoKey](
  "commit" -> ("git rev-parse HEAD"!!).trim
)

sourceGenerators in Compile <+= buildInfo

buildInfoPackage := "io.netflow.lib"

addArtifact(Artifact("netflow", "server"), assembly)

net.virtualvoid.sbt.graph.Plugin.graphSettings

