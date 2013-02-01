import AssemblyKeys._
import scalariform.formatter.preferences._

name := "netflow"

organization := "io.wasted"

version := ("git describe --always"!!).trim

scalaVersion := "2.10.0"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-optimise", "-Yinline-warnings")

scalariformSettings

ScalariformKeys.preferences := FormattingPreferences().setPreference(AlignParameters, true)

assemblySettings

jarName in assembly := "netflow-" + ("git describe --always"!!).trim + ".jar"

mainClass in assembly := Some("io.netflow.Server")

buildInfoSettings

sourceGenerators in Compile <+= buildInfo

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion) ++ Seq[BuildInfoKey](
  "commit" -> ("git rev-parse HEAD"!!).trim
)

buildInfoPackage := "io.netflow.lib"

resolvers ++= Seq(
  "Twitter's Repository" at "http://maven.twttr.com/",
  "Kungfuters" at "http://maven.kungfuters.org/content/groups/public/",
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
  "Typesafe Ivy Repo" at "http://repo.typesafe.com/typesafe/ivy-releases",
  "Java.net Maven2 Repository" at "http://download.java.net/maven/2/"
)


libraryDependencies ++= Seq(
  "io.wasted" %% "wasted-util" % "0.4.1",
  "ch.qos.logback" % "logback-classic" % "1.0.7" % "compile",
  "org.specs2" %% "specs2" % "1.13" % "test",
  "com.github.spullara.redis" % "client" % "0.3"
)

