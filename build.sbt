name := "hackathon-knight"

version := "1.0.0"

scalaVersion := "2.10.5"

dependencyOverrides += "org.scala-lang" % "scala-library" % scalaVersion.value

scalacOptions ++= Seq("-unchecked", "-deprecation")

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.2"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.2"

libraryDependencies += "net.liftweb" %% "lift-json" % "2.5.3"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.0.0"

libraryDependencies += "net.sf.opencsv" % "opencsv" % "2.3"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.5"

crossPaths := false
