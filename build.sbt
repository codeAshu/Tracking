name := "Tracking"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies +=  "org.apache.spark" %% "spark-core" % "1.2.0"

libraryDependencies +=  "org.apache.spark" %% "spark-sql" % "1.2.0"

libraryDependencies +=  "org.apache.spark" %% "spark-streaming" % "1.2.0"

libraryDependencies +=  "org.apache.spark" %% "spark-mllib" % "1.2.0"

libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.2.0"

libraryDependencies += "org.optaplanner" % "optaplanner" % "6.2.0.Final"

libraryDependencies += "com.thoughtworks.xstream" % "xstream" % "1.1.3"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

libraryDependencies += "postgresql" % "postgresql" % "9.1-901-1.jdbc4"

