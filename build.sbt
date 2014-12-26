name := "spark-sql-hbase"

version := "0.1"

organization := "com.shengli"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.2.0" % "provided"

libraryDependencies += "org.apache.hbase" % "hbase" % "0.94.14"

libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.4.5"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.6.1"


publishMavenStyle := true

pomExtra := (
  <url>https://github.com/OopsOutOfMemory/spark-sql-hbase</url>
  <scm>
    <url>git@github.com:OopsOutOfMemory/spark-sql-hbase.git</url>
    <connection>scm:git:git@github.com:OopsOutOfMemory/spark-hbase.git</connection>
  </scm>
  <developers>
    <developer>
      <id>OopsOutOfMemory</id>
      <name>Sheng,Li(盛利)</name>
      <url>https://github.com/OopsOutOfMemory</url>
    </developer>
  </developers>)

// Enable Junit testing.
// libraryDependencies += "com.novocode" % "junit-interface" % "0.9" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"
