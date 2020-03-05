/** Question 71
  * Problem Scenario 31 : You have given following two files
  * 1. Content.txt: Contain a huge text file containing space separated words.
  * 2. Remove.txt: Ignore/filter all the words given in this file (Comma Separated).
  * Write a Spark program which reads the Content.txt file and load as an RDD, remove all the
  * words from a broadcast variables (which is loaded as an RDD of words from Remove.txt).
  * And count the occurrence of the each word and save it as a text file in HDFS.
  * Content.txt
  * Hello this is ABCTech.com
  * This is TechABY.com
  * Apache Spark Training
  * This is Spark Learning Session
  * Spark is faster than MapReduce
  * Remove.txt
  * Hello, is, this, the
  *
  * We have to create the files and put them into HDFS
  * $ gedit /home/cloudera/files/Content.txt
  * $ gedit /home/cloudera/files/Remove.txt
  * $ hdfs dfs -put /home/cloudera/files/Content.txt /user/cloudera/files
  * $ hdfs dfs -put /home/cloudera/files/Remove.txt /user/cloudera/files
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object question71 {

  val spark = SparkSession
    .builder()
    .appName("question71")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question71")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val inputPath = "hdfs://quickstart.cloudera/user/cloudera/files/"
  val output = "hdfs://quickstart.cloudera/user/cloudera/exercises/question_71"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val l = List("", " ")
      val remove = sc
        .textFile(s"${inputPath}Remove.txt")
        .map(line => line.split(","))
        .collect

      val broadcast = sc
        .broadcast(remove(0).toList.map(v => v.trim) ::: l)

      val content = sc
        .textFile(s"${inputPath}Content.txt")
        .flatMap(line => line.split(" "))
        .filter(w => broadcast.value.contains(w) == false)
        .map(w => (w, 1))
        .reduceByKey( (v, c) => v + c)
        .sortBy(t => t._2, false)

      content
        .saveAsTextFile(output)

      /**
        * Check the results
        * $ hdfs dfs -cat /user/cloudera/exercises/question_71/part*
        */

      // To have the opportunity to view the web console of Spark: http://localhost:4040/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("SparkContext stopped.")
      spark.stop()
      println("SparkSession stopped.")
    }
  }
}

/*
$ gedit /home/cloudera/files/Content.txt
$ gedit /home/cloudera/files/Remove.txt
$ hdfs dfs -put /home/cloudera/files/Content.txt /user/cloudera/files
$ hdfs dfs -put /home/cloudera/files/Remove.txt /user/cloudera/files

val l = List("", " ")
val remove = sc.textFile("/user/cloudera/files/Remove.txt").map(line => line.split(",")).collect
val broadcast = sc.broadcast(remove(0).toList.map(v => v.trim))
val content = sc.textFile("/user/cloudera/files/Content.txt").flatMap(line => line.split(" ")).filter(w => !l.contains(w)).filter(w => !broadcast.value.contains(w))
val wordCount = content.map(w => (w,1)).reduceByKey( (v,c) => v + c).sortBy(t => t._2, false)
wordCount.saveAsTextFile("/user/cloudera/question71/result")

$ hdfs dfs -cat /user/cloudera/question71/result/part*
*/