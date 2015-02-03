name := "Tracking"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies +=  "org.apache.spark" %% "spark-core" % "1.2.0"

libraryDependencies +=  "org.apache.spark" %% "spark-sql" % "1.2.0"
  
libraryDependencies +=  "org.apache.spark" %% "spark-streaming" % "1.2.0"

libraryDependencies +=  "org.apache.spark" %% "spark-mllib" % "1.2.0"

libraryDependencies += "postgresql" % "postgresql" % "9.1-901-1.jdbc4"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "1.6.0"

libraryDependencies += "joda-time" % "joda-time" % "2.7"

libraryDependencies += "org.scalaj" %% "scalaj-time" % "0.7"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
