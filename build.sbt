import AssemblyKeys._
import scalariform.formatter.preferences._

name := "netflow"

organization := "io.wasted"

version := scala.io.Source.fromFile("version").mkString.trim

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-Yinline-warnings", "-Xcheckinit", "-encoding", "utf8", "-feature")

scalacOptions ++= Seq("-language:higherKinds", "-language:postfixOps", "-language:implicitConversions", "-language:reflectiveCalls", "-language:existentials")

javacOptions ++= Seq("-target", "1.7", "-source", "1.7", "-Xlint:deprecation")

mainClass in assembly := Some("io.netflow.Node")

libraryDependencies ++= {
  val wastedVersion = "0.9.1"
  val liftVersion = "2.5.1"
  val phantomVersion = "1.2.2"
  Seq(
    "io.wasted" %% "wasted-util" % wastedVersion,
    "com.websudos"  %% "phantom-dsl" % phantomVersion,
    "net.liftweb" %% "lift-json" % liftVersion,
    "joda-time" % "joda-time" % "2.3",
    "org.joda" % "joda-convert" % "1.4"
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

resolvers ++= Seq(
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
  "wasted.io/repo" at "http://repo.wasted.io/mvn",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Twitter's Repository" at "http://maven.twttr.com/",
  "Typesafe Ivy Repo" at "http://repo.typesafe.com/typesafe/ivy-releases",
  "Java.net Maven2 Repository" at "http://download.java.net/maven/2/"
)

