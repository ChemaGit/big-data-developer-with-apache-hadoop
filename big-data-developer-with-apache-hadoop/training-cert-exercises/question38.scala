/** Question 38
  * Problem Scenario GG : You have been given below code snippet.
  * val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
  * val b = a.keyBy(_.length)
  * val c = sc.parallelize(List("ant", "falcon", "squid"), 2)
  * val d = c.keyBy(_.length)
  * operation1
  * Write a correct code snippet for operation1 which will produce desired output, shown below.
  * Array[(Int, String)] = Array((4,lion))
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object question38 {

  val spark = SparkSession
    .builder()
    .appName("question38")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question38")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext


  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
      val b = a.keyBy(_.length)
      val c = sc.parallelize(List("ant", "falcon", "squid"), 2)
      val d = c.keyBy(_.length)
      val e = b.subtractByKey(d)

      e.foreach(println)

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
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
val b = a.keyBy(_.length)
val c = sc.parallelize(List("ant", "falcon", "squid"), 2)
val d = c.keyBy(_.length)

b.subtractByKey(d).collect
// res3: Array[(Int, String)] = Array((4,lion))
*/