/** Question 56
  * Problem Scenario 47 : You have been given below code snippet, with intermediate output.
  * val z = sc.parallelize(List(1,2,3,4,5,6), 2)
  * // lets first print out the contents of the RDD with partition labels
  * def myfunc(index: Int, iter: lterator[(Int)]): Iterator[String] = {
  * iter.toList.map(x => "[partID: " + index + ", val: " + x + "]").iterator
  * //In each run , output could be different, while solving problem assume belowm output only.
  * z.mapPartitionsWithIndex(myfunc).collect
  * res28: Array[String] = Array([partID:0, val: 1], [partID:0, val: 2], [partID:0, val: 3], [partID:1,val: 4], [partID:1, val: 5], [partID:1, val: 6])
  * Now apply aggregate method on RDD z , with two reduce function , first will select max value in each partition and second will add all the maximum values from all partitions.
  * Initialize the aggregate with value 5. hence expected output will be 16.
  */

/** Question 56
  * Problem Scenario 47 : You have been given below code snippet, with intermediate output.
  * val z = sc.parallelize(List(1,2,3,4,5,6), 2)
  * // lets first print out the contents of the RDD with partition labels
  * def myfunc(index: Int, iter: lterator[(Int)]): Iterator[String] = {
  * iter.toList.map(x => "[partID: " + index + ", val: " + x + "]").iterator
  * //In each run , output could be different, while solving problem assume belowm output only.
  * z.mapPartitionsWithIndex(myfunc).collect
  * res28: Array[String] = Array([partID:0, val: 1], [partID:0, val: 2], [partID:0, val: 3], [partID:1,val: 4], [partID:1, val: 5], [partID:1, val: 6])
  * Now apply aggregate method on RDD z , with two reduce function , first will select max value in each partition and second will add all the maximum values from all partitions.
  * Initialize the aggregate with value 5. hence expected output will be 16.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object question56 {

  val spark = SparkSession
    .builder()
    .appName("question56")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question56")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def myFunc(index: Int, iter: Iterator[Int]): Iterator[String] = {
    iter.toList.map(x => s"[partID]: $index, val: $x]").iterator
  }

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val z = sc.parallelize(List(1,2,3,4,5,6), 2)

      z.mapPartitionsWithIndex(myFunc)
        .collect
        .foreach(print)
      // [partID]: 0, val: 1][partID]: 0, val: 2][partID]: 0, val: 3][partID]: 1, val: 4][partID]: 1, val: 5][partID]: 1, val: 6]

      val agg = z
        .aggregate(5)(((z: Int, v: Int) => z.max(v)), ((c: Int, v: Int) => c + v))

      println()

      println(s"agg: $agg")

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
val z = sc.parallelize(List(1,2,3,4,5,6), 2)

def myfunc(index: Int, iter: Iterator[(Int)]): Iterator[String] = {
  iter.toList.map(x => "[partID: " + index + ", val: " + x + "]").iterator
}
z.mapPartitionsWithIndex(myfunc).collect
// res28: Array[String] = Array([partID:0, val: 1], [partID:0, val: 2], [partID:0, val: 3], [partID:1,val: 4], [partID:1, val: 5], [partID:1, val: 6])

val agg = z.aggregate(5)( ( (z: Int, v: Int) => z.max(v)) , ( (c: Int, v: Int) => c + v) )
// agg: Int = 16
*/