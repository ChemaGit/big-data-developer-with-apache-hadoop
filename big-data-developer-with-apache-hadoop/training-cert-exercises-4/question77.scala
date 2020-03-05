/** Question 77
  * Problem Scenario 51 : You have been given below code snippet.
  * val a = sc.parallelize(List(1, 2,1, 3), 1)
  * val b = a.map((_, "b"))
  * val c = a.map((_, "c"))
  * Operation_xyz
  * Write a correct code snippet for Operation_xyz which will produce below output.
  * Output:
  * Array[(Int, (Iterable[String], Iterable[String]))] = Array(
  * (2,(ArrayBuffer(b),ArrayBuffer(c))),
  * (3,(ArrayBuffer(b),ArrayBuffer(c))),
  * (1,(ArrayBuffer(b, b),ArrayBuffer(c, c)))
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object question77 {

  val spark = SparkSession
    .builder()
    .appName("question77")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question77")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val a = sc.parallelize(List(1, 2,1, 3), 1)
      val b = a.map((_, "b"))
      val c = a.map((_, "c"))
      //cogroup [Pair], groupWith [Pair] A very powerful set of functions that allow grouping up to 3 key-value RDDs together using their keys.
      val res = b.cogroup(c).collect
      res.foreach(r => println(r))

      println("*********************************")

      // Alternative solution
      val res2 = b.groupByKey().join(c.groupByKey()).collect
      res2.foreach(r => println(r))

      println("********************************")

      // Another example
      val x = sc.parallelize(List((1, "apple"), (2, "banana"), (3, "orange"), (4, "kiwi")), 2)
      val y = sc.parallelize(List((5, "computer"), (1, "laptop"), (1, "desktop"), (4, "iPad")), 2)
      val res3 = x.cogroup(y).collect
      // Array[(Int, (Iterable[String], Iterable[String]))] = Array( (4,(ArrayBuffer(kiwi),ArrayBuffer(iPad))), (2,(ArrayBuffer(banana),ArrayBuffer())),
      // (3,(ArrayBuffer(orange),ArrayBuffer())), (1 ,(ArrayBuffer(apple),ArrayBuffer(laptop, desktop))), (5,(ArrayBuffer(),ArrayBuffer(computer))))
      res3.foreach(r => println(r))

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

/**SOLUTION IN THE SPARK REPL
val a = sc.parallelize(List(1, 2,1, 3), 1)
val b = a.map((_, "b"))
val c = a.map((_, "c"))
//cogroup [Pair], groupWith [Pair] A very powerful set of functions that allow grouping up to 3 key-value RDDs together using their keys.
b.cogroup(c).collect

// Alternative solution
b.groupByKey().join(c.groupByKey()).collect

// Another example
val x = sc.parallelize(List((1, "apple"), (2, "banana"), (3, "orange"), (4, "kiwi")), 2)
val y = sc.parallelize(List((5, "computer"), (1, "laptop"), (1, "desktop"), (4, "iPad")), 2)
x.cogroup(y).collect
// Array[(Int, (Iterable[String], Iterable[String]))] = Array( (4,(ArrayBuffer(kiwi),ArrayBuffer(iPad))), (2,(ArrayBuffer(banana),ArrayBuffer())),
// (3,(ArrayBuffer(orange),ArrayBuffer())), (1 ,(ArrayBuffer(apple),ArrayBuffer(laptop, desktop))), (5,(ArrayBuffer(),ArrayBuffer(computer))))
*/