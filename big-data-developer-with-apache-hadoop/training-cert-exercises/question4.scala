/**
  * /** Question 4
  * * Problem Scenario 58 : You have been given below code snippet.
  * * val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
  * * val b = a.keyBy(_.length)
  * * operation1
  * * Write a correct code snippet for operation1 which will produce desired output, shown below.
  * * Array[(Int, Seq[String])] = Array((4,ArrayBuffer(lion)), (6,ArrayBuffer(spider)),(3,ArrayBuffer(dog, cat)), (5,ArrayBuffer(tiger, eagle)))
  **/
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object question4 {

  val spark = SparkSession
    .builder()
    .appName("question4")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question4")  // To silence Metrics warning
    .getOrCreate()
  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
      val b = a.keyBy(_.length)
      val c = b.groupByKey
        .collect
        .foreach(x => println(x._1 + ", " + x._2.mkString(",")))
      // res1: Array[(Int, Iterable[String])] = Array((4,CompactBuffer(lion)), (6,CompactBuffer(spider)), (3,CompactBuffer(dog, cat)), (5,CompactBuffer(tiger, eagle)))

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

/* SOLUTION IN THE SPARK REPL
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
val b = a.keyBy(_.length)
// Solution
b.groupByKey().collect
// res0: Array[(Int, Iterable[String])] = Array((4,CompactBuffer(lion)), (6,CompactBuffer(spider)), (3,CompactBuffer(dog, cat)), (5,CompactBuffer(tiger, eagle)))

//output
b.map({case(k,i) => "%d -->%s".format(k,i.mkString("(","",")"))}).saveAsTextFile("/user/cloudera/question4/result_text")

$ hdfs dfs -ls /user/cloudera/question4/result_text
$ hdfs dfs -cat /user/cloudera/question4/result_text/part*
*/
