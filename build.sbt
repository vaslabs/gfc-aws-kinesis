name := "gfc-aws-kinesis"

organization := "com.gilt"

scalaVersion := "2.11.6"

crossScalaVersions := Seq("2.11.6", "2.10.5")

libraryDependencies ++= Seq(
  "com.gilt" %% "gfc-logging" % "0.0.2"
, "com.gilt" %% "gfc-concurrent" % "0.1.0"
, "com.amazonaws" % "aws-java-sdk-kinesis" % "1.9.16"
, "org.specs2" %% "specs2-scalacheck" % "2.3.12" % Test
)

releaseCrossBuild := true

releasePublishArtifactsAction := PgpKeys.publishSigned.value

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

licenses := Seq("Apache-style" -> url("https://raw.githubusercontent.com/gilt/gfc-aws-kinesis/master/LICENSE"))

homepage := Some(url("https://github.com/gilt/gfc-aws-kinesis"))

pomExtra := (
  <scm>
    <url>https://github.com/gilt/gfc-aws-kinesis.git</url>
    <connection>scm:git:git@github.com:gilt/gfc-aws-kinesis.git</connection>
  </scm>
  <developers>
    <developer>
      <id>andreyk0</id>
      <name>Andrey Kartashov</name>
      <url>https://github.com/andreyk0</url>
    </developer>
    <developer>
      <id>krschultz</id>
      <name>Kevin Schultz</name>
      <url>https://github.com/krschultz</url>
    </developer>
  </developers>
)
