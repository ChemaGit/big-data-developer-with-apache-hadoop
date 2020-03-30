package spark.spark-streaming

/**
  * // download data from IBM Servier
  * // this may take ~30 seconds depending on your internet speed
  * //"wget --quiet https://cocl.us/BD0211EN_Data" !
  * //println("Data Downloaded!")
  * // unzip the folder's content into "resources" directory
  * //"unzip -q -o -d /resources/jupyterlab/labs/BD0211EN/ BD0211EN_Data" !
  * //println("Data Extracted!")
  */

/**
  * taxi trip data will be streamed using a socket connection
  * and then analyzed to provide a summary of number of passengers by taxi vendor.
  *
  * There are two relevant files for this section. The first one is the nyctaxi100.csv which will serve as the source of the stream.
  * The other file is a python file, taxistreams.py, which will feed the csv file through a socket connection to simulate a stream.
  *
  * $ cd /home/cloudera/CognitiveClass/data/LabData/
  * $ python taxistreams.py
  *
  * Once started, the program will bind and listen to the localhost socket 7777.
  * When a connection is made, it will read ‘nyctaxi100.csv’ and send across the socket.
  * The sleep is set such that one line will be sent every 0.5 seconds, or 2 rows a second.
  * This was intentionally set to a high value to make it easier to view the data during execution.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._

object CreatingSparkAppUsingSparkStreaming {
  val spark = SparkSession
    .builder()
    .appName("CreatingSparkAppUsingSparkStreaming")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "CreatingSparkAppUsingSparkStreaming")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val input = "/home/cloudera/CognitiveClass/data/LabData/nyctaxisub.csv"

  def main(args: Array[String]): Unit = {
    // Turn off logging so that you can see the output of the application
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      // Create the StreamingContext by using the existing SparkContext (sc).
      // It will be using a 1 second batch interval,
      // which means the stream is divided to 1 second batches and each batch becomes a RDD.
      // This is intentional to make it easier to read the data during execution.
      val ssc = new StreamingContext(sc, Seconds(1))

      // Create the socket stream that connects to the localhost socket 7777.
      // This matches the port that the Python script is listening on.
      // Each batch from the Stream be a lines RDD.
      val lines = ssc.socketTextStream("localhost", 7777)

      // mapping pass(15), which is the vendor, and pass(7), which is the passenger count.
      // Then this is reduced by key resulting in a summary of number of passengers by vendor.
      val pass = lines.map(_.split(",")).
        map(pass => (pass(15), pass(7).toInt)).
        reduceByKey(_+_)

      pass.print()

      ssc.start()
      ssc.awaitTermination()
    } finally {
      sc.stop()
      println("SparkContext stopped.")
      spark.stop()
      println("SparkSession stopped.")
    }
  }
}