/** Question 75
  * Problem Scenario 54 : You have been given below code snippet.
  * val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"))
  * val b = a.map(x => (x.length, x))
  * operation1
  * Write a correct code snippet for operation1 which will produce desired output, shown below.
  * Array[(Int, String)] = Array((4,lion), (7,panther), (3,dogcat), (5,tigereagle))
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object question75 {

  val spark = SparkSession
    .builder()
    .appName("question75")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question75")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"))
      val b = a.map(x => (x.length, x))

      val operation = b
        .reduceByKey( (v, c) => s"$v$c")

      operation.foreach(println)

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
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"))
val b = a.map(x => (x.length, x))
b.reduceByKey( (v, c) => v + c).collect
// res0: Array[(Int, String)] = Array((4,lion), (5,tigereagle), (3,dogcat), (7,panther))

// another solution could be
b.foldByKey("")( (v,c) => v + c).collect
//res1: Array[(Int, String)] = Array((4,lion), (5,tigereagle), (3,dogcat), (7,panther))
*/