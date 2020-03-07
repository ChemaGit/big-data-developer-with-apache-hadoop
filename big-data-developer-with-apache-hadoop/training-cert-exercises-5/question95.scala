/** Question 95
  * Problem Scenario 59 : You have been given below code snippet.
  * val x = sc.parallelize(1 to 20)
  * val y = sc.parallelize(10 to 30)
  * operation1
  * z.collect
  * Write a correct code snippet for operation1 which will produce desired output, shown below.
  * Array[Int] = Array(16,12, 20,13,17,14,18,10,19,15,11)
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object question95 {

  val spark = SparkSession
    .builder()
    .appName("question95")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question95")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val x = sc.parallelize(1 to 20)
      val y = sc.parallelize(10 to 30)

      val result = x.intersection(y)

      result.collect.foreach(println)

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
val x = sc.parallelize(1 to 20)
val y = sc.parallelize(10 to 30)
val z = x.intersection(y)
z.collect
// res0: Array[Int] = Array(16, 12, 20, 13, 17, 14, 18, 10, 19, 15, 11)
*/