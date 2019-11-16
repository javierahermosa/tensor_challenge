name := "tensor_challenge"

version := "0.1"

scalaVersion := "2.11.12"

//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= Seq(
  // spark core
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  //"org.apache.spark" %% "spark-hive" % "2.4.4" % "provided",

  // spark-modules
  // "org.apache.spark" %% "spark-graphx" % "1.6.0",
  // "org.apache.spark" %% "spark-mllib" % "1.6.0",
  // "org.apache.spark" %% "spark-streaming" % "1.6.0",

  // spark packages
  "com.databricks" %% "spark-csv" % "1.3.0",

  // testing
  "org.scalatest"   %% "scalatest"    % "3.0.8"   % "test",
  "org.scalacheck"  %% "scalacheck"   % "1.12.2"  % "test",
  "com.github.mrpowers" % "spark-fast-tests" % "v0.16.0" % "test",

  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1",

  // machine learning
  "org.apache.spark" %% "spark-mllib" % "2.4.0"
)
