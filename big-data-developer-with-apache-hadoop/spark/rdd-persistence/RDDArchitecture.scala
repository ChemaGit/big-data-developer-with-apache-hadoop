package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object RDDArchitecture {

  val spark = SparkSession
    .builder()
    .appName("RDDArchitecture")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "RDDArchitecture")  // To silence Metrics warning
    .config("spark.scheduler.mode", "FAIR") // To schedule concurrent jobs, default FIFO, FAIR is more appropiate for multiusers
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      // create the RDD of the trips
      val input1 = sc.textFile("hdfs://quickstart.cloudera/public/cognitive_class/trips/*")
      val header1 = input1.first // to skip the header row
      val trips = input1
        .filter(line => line != header1)
        .map(line => line.split(","))

      //      trips.take(20).foreach(x => println(x.mkString(",")))

      // Create the RDD for the stations.
      val input2 = sc.textFile("hdfs://quickstart.cloudera/public/cognitive_class/stations/*")
      val header2 = input2.first // to skip the header row
      //      val stations = input2
      //        .filter(line => line != header2)
      //        .map(line => line.split(","))
      //        .keyBy(r => r(0).toInt)
      //      stations.take(20).foreach(x => println(x._2.mkString(",")))

      // How could we optimize this? Because we're keying all the RDDs by the station id and joining
      // against the same station RDD, we should use a HashPartitioner on the stations RDD to allow for
      // effective partitioning. To do so, use the partitionBy function along with the HashPartitioner to
      // partition by the same number as the trips RDD.
      val stations = input2
        .filter(line => line != header2)
        .map(line => line.split(","))
        .keyBy(r => r(0).toInt)
        .partitionBy(new HashPartitioner(trips.partitions.size))

      // To create the RDD of the startTrips and the endTrips, join the stations RDD with each of the
      // trips RDD. Remember to use the keyBy function on each of the original trips RDD. The index of
      // the station id of each the start and end trip is 4 and 7, respectively.
      val startTrips = stations
        .join(trips.keyBy(r => r(4).toInt))
        .cache()

      val endTrips = stations
        .join(trips.keyBy(r => r(7).toInt))
        .cache()

      println(startTrips.toDebugString)
      println(endTrips.toDebugString)

      println(startTrips.count())
      println(endTrips.count())

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