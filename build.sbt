name := "rocksdb-poc"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.rocksdb" % "rocksdbjni" % "4.11.2",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test"
)
