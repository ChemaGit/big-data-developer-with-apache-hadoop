package spark.rdd-persistence

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object PersistenceAndMemoryTuning {

  val spark = SparkSession
    .builder()
    .appName("PersistenceAndMemoryTuning")
    .master("local[*]")
    .enableHiveSupport()
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "PersistenceAndMemoryTuning")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val input = "hdfs://quickstart.cloudera/public/cognitive_class/"

  def benchmark(rdd: RDD[(Int, Int)]): Long = {
    val start = System.currentTimeMillis()
    rdd.collect()
    val end = System.currentTimeMillis()
    (end - start)
  }

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val input1 = sc.textFile(s"${input}trips")
      val header1 = input1.first // to skip the header row
      val trips = input1
        .filter(_ != header1)
        .map(_.split(","))
        .map(utils.Trip.parse(_))

      // calculate the average duration of trips both by start and end terminals in the trips RDD

      val durationsByStart = trips
        .keyBy(_.startTerminal)
        .mapValues(_.duration)

      val durationsByEnd = trips
        .keyBy(_.endTerminal)
        .mapValues(_.duration)

      val resultsStart = durationsByStart
        .aggregateByKey((0, 0))((acc, value) => (acc._1 + value, acc._2 + 1),
          (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      val avgStart = resultsStart.mapValues(i => i._1 / i._2)

      val resultsEnd = durationsByEnd
        .aggregateByKey((0, 0))((acc, value) => (acc._1 + value, acc._2 + 1),
          (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      val avgEnd = resultsEnd.mapValues(i => i._1 / i._2)

      val timeWithoutCaching = benchmark(avgStart) + benchmark(avgEnd)
      println(s"Duration without Caching: $timeWithoutCaching")

      // call cache() or persist(StorageLevel.MEMORY_ONLY).
      // Run the app with that new line of code right after the stations RDD.
      trips.persist(StorageLevel.MEMORY_ONLY)

      val timeWithCaching = benchmark(avgStart) + benchmark(avgEnd)
      println(s"Duration with Caching: $timeWithCaching")

      // Now re-run, but with a different storage level:
      trips.unpersist()

      trips.persist(StorageLevel.MEMORY_ONLY_SER)

      // check out the Spark UI (on port 4040).
      // You'll noticed the top one (recent) uses less memory, why?
      val timeWithCachingAndSer = benchmark(avgStart) + benchmark(avgEnd)
      println(s"Duration with Caching and Serialization: $timeWithCachingAndSer")

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