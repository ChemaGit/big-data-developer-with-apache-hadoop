name := "apache-spark-2.0-scala"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "com.typesafe" % "config" % "1.3.2"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.25"

dependencyOverrides += "junit" % "junit" % "4.10" % Test

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0-SNAP3"


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.2.0" //% "provided"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.0.0"
libraryDependencies += "org.apache.kafka" %% "kafka" % "1.1.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0" //% "runtime"

// https://mvnrepository.com/artifact/com.databricks/spark-avro
libraryDependencies += "com.databricks" %% "spark-avro" % "4.0.0"
// https://mvnrepository.com/artifact/databricks/spark-csv
libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.12"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.5"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.5"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.5"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0-SNAP3" % Test

// https://mvnrepository.com/artifact/com.holdenkarau/spark-testing-base
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % Test

/*
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.8"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.8"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.8"
libraryDependencies += "org.apache.hbase" % "hbase-protocol" % "1.1.8"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.5"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.5"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.5"
 */
//(unmanagedResources in Compile) := (unmanagedResources in Compile).value.filter(name => List("exercises_cert_1","exercises_cert").contains(name) )
/*excludeFilter in unmanagedResources := {
  val public = ((resourceDirectory in Compile).value / "exercises_cert").getCanonicalPath
  new SimpleFileFilter(_.getCanonicalPath startsWith public)
}*/

// excludeFilter in unmanagedSources := HiddenFileFilter || "*exercise*"