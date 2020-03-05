/** Question 53
  * Problem Scenario 69 : Write down a Spark Application using Scala,
  * In which it read a file "Content.txt" (On hdfs) with following content.
  * And filter out the word which is less than 3 characters and ignore all empty lines.
  * Once done store the filtered data in a directory called "question53" (On hdfs)
  * Content.txt
  * Hello this is ABCTECH.com
  * This is ABYTECH.com
  * Apache Spark Training
  * This is Spark Learning Session
  * Spark is faster than MapReduce
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object question53 {

  val spark = SparkSession
    .builder()
    .appName("question53")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question53")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/files/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val filt = sc.broadcast(List("", " "))

      val content = sc
        .textFile(s"${path}Content.txt")
        .flatMap(line => line.split(" "))
        .filter(w => filt.value.contains(w) == false)
        .filter(w => w.length > 2)
        .cache()

      content
        .collect
        .foreach(r => println(r.mkString("")))

      content
        .saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/exercise_4")

      // check the results
      // hdfs dfs -ls /user/cloudera/exercise_4
      // hdfs dfs -cat /user/cloudera/exercise_4/part-00000

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
val filt = List("", " ")
val content = sc.textFile("/user/cloudera/files/Content.txt").flatMap(line => line.split(" ")).filter(w => !filt.contains(w)).filter(w => w.length > 2)
content.saveAsTextFile("/user/cloudera/question53")

$ hdfs dfs -ls /user/cloudera/question53
$ hdfs dfs -cat /user/cloudera/question53/part-00000
*/