import AssemblyKeys._
import scalariform.formatter.preferences._

name := "netflow"

organization := "io.wasted"

version := scala.io.Source.fromFile("version").mkString.trim

scalaVersion := "2.10.0"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-optimise", "-Yinline-warnings")

mainClass in assembly := Some("io.netflow.Server")

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

libraryDependencies ++= Seq(
  "io.wasted" %% "wasted-util" % "0.5.0-SNAPSHOT",
  "ch.qos.logback" % "logback-classic" % "1.0.7" % "compile",
  "org.specs2" %% "specs2" % "1.13" % "test",
  "com.github.spullara.redis" % "client" % "0.3"
)

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

