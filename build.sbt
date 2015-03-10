name := "Tracking"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies +=  "org.apache.spark" %% "spark-core" % "1.2.0"

libraryDependencies +=  "org.apache.spark" %% "spark-sql" % "1.2.0"

libraryDependencies +=  "org.apache.spark" %% "spark-streaming" % "1.2.0"

libraryDependencies +=  "org.apache.spark" %% "spark-mllib" % "1.2.0"

libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.2.0"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

libraryDependencies += "postgresql" % "postgresql" % "9.1-901-1.jdbc4"

libraryDependencies += "joda-duration" % "joda-duration" % "2.7"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
