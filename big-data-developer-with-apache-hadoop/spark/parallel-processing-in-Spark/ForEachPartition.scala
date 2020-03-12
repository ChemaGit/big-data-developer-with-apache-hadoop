/**
  * use foreachPartition to print out the first record of each partition
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ForEachPartition {

  val spark = SparkSession
    .builder()
    .appName("ForEachPartition")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "ForEachPartition") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def printFirstLine(iter: Iterator[Any]) = {
    println(iter.next)
  }

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val accounts=sc.textFile("/loudacre/accounts/*")
      accounts.foreachPartition(printFirstLine)

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
