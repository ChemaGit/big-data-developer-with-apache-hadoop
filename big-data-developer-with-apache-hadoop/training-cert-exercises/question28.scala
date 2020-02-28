/** Question 28
  * Problem Scenario 43 : You have been given following code snippet.
  * val grouped = sc.parallelize( Seq( ( (1,"two"), List((3,4), (5,6)) ) ) )
  * val flattened = grouped.flatMap {A => groupValues.map { value => B } }
  * You need to generate following output.
  * Hence replace A and B
  * Array((1,two,3,4),(1,two,5,6))
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object question28 {

  val spark = SparkSession
    .builder()
    .appName("question28")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question28")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)


    try {
      val grouped = sc.parallelize( Seq( ( (1,"two"), List((3,4), (5,6)) ) ) )

      val flattened = grouped
        .flatMap({case(s,groupValues) => groupValues.map{value => (s._1, s._2,value._1, value._2)}})

      flattened
        .collect
        .foreach(x => println(x))
      // res2: Array[(Int, String, Int, Int)] = Array((1,two,3,4), (1,two,5,6))

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
val grouped = sc.parallelize( Seq( ( (1,"two"), List((3,4), (5,6)) ) ) )
val flattened = grouped.flatMap{case(v: (Int, String), groupValues:List[(Int,Int)]) => groupValues.map{value => (v._1,v._2,value._1,value._2)}}
flattened.collect

// res2: Array[(Int, String, Int, Int)] = Array((1,two,3,4), (1,two,5,6))
*/