/** Question 68
  * Problem Scenario 68 : You have given a file as below.
  * hdfs://quickstart.cloudera/user/cloudera/files/file111.txt
  * File contain some text. As given Below
  * hdfs://quickstart.cloudera/user/cloudera/files/file111.txt
  * Apache Hadoop is an open-source software framework written in Java for distributed storage and distributed processing of very large data sets on computer clusters built from commodity hardware.
  * All the modules in Hadoop are designed with a fundamental assumption that hardware failures are common and should be automatically handled by the framework.
  * The core of Apache Hadoop consists of a storage part known as Hadoop Distributed File System (HDFS) and a processing part called MapReduce. Hadoop splits files into large blocks and distributes them across nodes in a cluster.
  * To process data, Hadoop transfers packaged code for nodes to process in parallel based on the data that needs to be processed.
  * His approach takes advantage of data locality nodes manipulating the data they have access to to allow the dataset to be processed faster and more efficiently than it would be in a more conventional supercomputer architecture that relies on a parallel file system where computation and data are distributed via high-speed networking.
  * For a slightly more complicated task, lets look into splitting up sentences from our documents into word bigrams. A bigram is pair of successive tokens in some sequence.
  * We will look at building bigrams from the sequences of words in each sentence, and then
  * Try to find the most frequently occuring ones.
  * The first problem is that values in each partition of our initial RDD describe lines from the file rather than sentences.
  * Sentences may be split over multiple lines.
  * The glom() RDD method is used to create a single entry for each document containing the list of all lines, we can then join the lines up, then resplit them into sentences using "." as the separator, using flatMap so that every object in our RDD is now a sentence.
  * A bigram is pair of successive tokens in some sequence. Please build bigrams from the sequences.
  *
  * Create the files in local filesystem and put it in HDFS
  *
  * $ gedit /home/cloudera/files/file111.txt &
  * $ hdfs dfs -put /home/cloudera/files/file111.txt /user/cloudera/files
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object question68 {

  val spark = SparkSession
    .builder()
    .appName("question68")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question68")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/files/"
  val output = "hdfs://quickstart.cloudera/user/cloudera/bigrams/result"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val l = sc.broadcast(List("", " "))

      val sentences = sc
        .textFile(s"${path}file111.txt")
        .glom()

      val joined = sentences
        .map(x => " ".concat(x.mkString("")))
        .flatMap(x => x.split('.'))

      val bigrams = joined
        .map(line => line.split("\\W")
          .filter(w => l.value.contains(w) == false))
        .flatMap(w => {for(i <- 0 until w.length -1)yield ( (w(i),w(i + 1)), 1)})
        .cache()

      bigrams
        .collect
        .foreach(println)

      val bigramsCount = bigrams
        .reduceByKey( (v,c) => v + c)
        .sortBy(t => t._2, false)
        .cache()

      bigramsCount
        .collect
        .foreach(println)

      bigramsCount.saveAsTextFile(output)

      bigrams.unpersist()
      bigramsCount.unpersist()

      /**
        * check the results
        * $ hdfs dfs -cat /user/cloudera/bigrams/result/part*
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

/*SOLUTION IN THE SPARK REPL
//Explanation: Solution :
//Step 1 : Create all three files in hdfs (We will do using Hue). However, you can first create in local filesystem and then upload it to hdfs.
$ gedit /home/cloudera/files/file111.txt &
$ hdfs dfs -put /home/cloudera/filesfile111.txt /user/cloudera/files

//Step 2 : The first problem is that values in each partition of our initial RDD describe lines from the file rather than sentences. Sentences may be split over multiple lines.
//The glom() RDD method is used to create a single entry for each document containing the list of all lines,
//we can then join the lines up, then resplit them into sentences using "." as the separator,
//using flatMap so that every object in our RDD is now a sentence.
val sentences = sc.textFile("/user/cloudera/files/file111.txt").glom()
val joined = sentences.map(x => " ".concat(x.mkString(""))).flatMap(x => x.split('.'))

//Step 3 : Now we have isolated each sentence we can split it into a list of words and extract the word bigrams from it.
//Our new RDD contains tuples containing the word bigram (itself a tuple containing the first and second word) as the first value and the number 1 as the second value.
val l = List("", " ")
val bigrams = joined.map(line => line.split("\\W").filter(w => !l.contains(w))).flatMap(w => {for(i <- 0 until w.length -1)yield ( (w(i),w(i + 1)), 1)})
bigrams.collect.foreach(println)
val bigramsCount = bigrams.reduceByKey( (v,c) => v + c).sortBy(t => t._2, false)
bigramsCount.collect.foreach(println)
bigramsCount.saveAsTextFile("/user/cloudera/question68/result")

$ hdfs dfs -cat /user/cloudera/question68/result/part*
*/