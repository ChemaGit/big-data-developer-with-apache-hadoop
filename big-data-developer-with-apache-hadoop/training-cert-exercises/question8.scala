/** Question 8
  * Problem Scenario 63 : You have been given below code snippet.
  * val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
  * val b = a.map(x => (x.length, x))
  * operation1
  * Write a correct code snippet for operation1 which will produce desired output, shown below.
  * Array[(Int, String)] = Array((4,lion), (3,dogcat), (7,panther), (5,tigereagle))
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object question8 {

  val spark = SparkSession
    .builder()
    .appName("question8")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question8")  // To silence Metrics warning
    .getOrCreate()
  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
      val b = a.map(x => (x.length, x))
      b
        .reduceByKey({case(v1, v2) => v1 + v2})
        .collect
        .foreach(println)

      // res10: Array[(Int, String)] = Array((4,lion), (3,dogcat), (7,panther), (5,tigereagle))

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
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
val b = a.map(x => (x.length, x))
// solution
b.reduceByKey((v,c) => v + c).collect
// res0: Array[(Int, String)] = Array((4,lion), (3,dogcat), (7,panther), (5,tigereagle))
 */