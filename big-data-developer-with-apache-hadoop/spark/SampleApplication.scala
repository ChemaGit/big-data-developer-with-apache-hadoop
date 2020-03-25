package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SampleApplication {

  val spark = SparkSession
    .builder()
    .appName("SampleApplication")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "SampleApplication")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val input = "hdfs://quickstart.cloudera/public/cognitive_class/trips"

  def benchmark(rdd: RDD[(String, Int)]): Long = {
    val start = System.currentTimeMillis()
    rdd.count()
    val end = System.currentTimeMillis()
    (end - start)
  }

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val taxi = sc.textFile(input)
      // To view the five rows of content, invoke the take function.
      taxi.take(5).foreach(println)

      val taxiParse = taxi.map(line=>line.split(","))

      val taxiMedKey = taxiParse.map(vals=>(vals(6), 1))

      taxiMedKey.take(5).foreach(println)

      val taxiMedCounts = taxiMedKey.reduceByKey((v1,v2)=>v1 + v2)

      taxiMedCounts.take(5).foreach(println)

      // Finally, the values are swapped so they can be ordered in descending order and the results are presented correctly.
      for (pair <-taxiMedCounts.map(_.swap).top(10)) println("Taxi Medallion %s had %s Trips".format(pair._2, pair._1))

      // While each step above was processed one line at a time, you can just as well process everything on one line:
      // val taxiMedCountsOneLine = taxi.map(line=>line.split(',')).map(vals=>(vals(6),1)).reduceByKey(_ + _)

      // Let's cache the taxiMedCountsOneLine to see the difference caching makes.
      // First, let's cache the RDD
      taxiMedCounts.cache()

      // Next, you have to invoke an action for it to actually cache the RDD.
      // Note the time it takes here
      println(s"Action taxiMedCounts.count() in miliseconds: ${benchmark(taxiMedCounts)}")

      // Run it again to see the difference.
      println(s"Action taxiMedCounts.count() in miliseconds: ${benchmark(taxiMedCounts)}")
      // The bigger the dataset, the more noticeable the difference will be.
      // In a sample file such as ours, the difference may be negligible.

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