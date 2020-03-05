/** Question 70
  * Problem Scenario 56 : You have been given below code snippet.
  * val a = sc.parallelize(1 to 100, 3)
  * operation1
  * Write a correct code snippet for operation1 which will produce desired output, shown below.
  * Array [Array [Int]] = Array(Array(1, 2, 3,4, 5, 6, 7, 8, 9,10,11,12,13,14,15,16,17,18,19, 20,21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33),
  * Array(34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55,56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66),
  * Array(67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100))
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object question70 {

  val spark = SparkSession
    .builder()
    .appName("question70")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question70")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)
    try {
      val a = sc.parallelize(1 to 100, 3)

      //glom Assembles an array that contains all elements of the partition and embeds it in an RDD. Each returned array contains the contents of one partition
      val b = a.glom().collect

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
val a = sc.parallelize(1 to 100, 3)
//glom Assembles an array that contains all elements of the partition and embeds it in an RDD. Each returned array contains the contents of one partition
a.glom().collect
// Array[Array[Int]] = Array(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33),
// Array(34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66),
// Array(67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100))
*/