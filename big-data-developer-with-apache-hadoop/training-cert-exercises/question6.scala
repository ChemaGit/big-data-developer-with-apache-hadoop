/** Question 6
  * Problem Scenario 65 : You have been given below code snippet.
  * val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
  * val b = sc.parallelize(1 to a.count.toInt, 2)
  * val c = a.zip(b)
  * operation1
  * Write a correct code snippet for operation1 which will produce desired output, shown below.
  * Array[(String, Int)] = Array((owl,3), (gnu,4), (dog,1), (cat,2), (ant,5))
  */


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object question6 {

  val spark = SparkSession
    .builder()
    .appName("question6")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question6")  // To silence Metrics warning
    .getOrCreate()
  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)


    try {
      val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
      val b = sc.parallelize(1 to a.count.toInt, 2)
      val c = a.zip(b)

      c.sortByKey(false)
        .collect
        .foreach(println)

      // res1: Array[(String, Int)] = Array((owl,3), (gnu,4), (dog,1), (cat,2), (ant,5))

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
val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
val b = sc.parallelize(1 to a.count.toInt, 2)
val c = a.zip(b)
// solution
c.sortByKey(false).collect
//res2: Array[(String, Int)] = Array((owl,3), (gnu,4), (dog,1), (cat,2), (ant,5))
*/
