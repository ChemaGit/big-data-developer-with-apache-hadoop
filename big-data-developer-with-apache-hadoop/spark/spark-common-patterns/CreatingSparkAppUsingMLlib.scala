package spark.spark-common-patterns

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
  * Spark will be used to acquire the K-Means clustering for drop-off latitudes and longitudes of taxis for 3 clusters.
  * The sample data contains a subset of taxi trips with hack license, medallion,
  * pickup date/time, drop off date/time, pickup/drop off latitude/longitude,
  * passenger count, trip distance, trip time and other information.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

object CreatingSparkAppUsingMLlib {

  val spark = SparkSession
    .builder()
    .appName("CreatingSparkAppUsingMLlib")
    .master("local[*]")
    .enableHiveSupport()
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "CreatingSparkAppUsingMLlib")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val input = "/home/cloudera/CognitiveClass/data/LabData/nyctaxisub.csv"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val taxiFile = sc.textFile(input)

      // Determine the number of rows in taxiFile.
      println(taxiFile.count())

      // Cleanse the data.
      val taxiData = taxiFile.filter(_.contains("2013")).
        filter(_.split(",")(3)!="" ).    //dropoff_latitude
        filter(_.split(",")(4)!="")      //dropoff_longitude

      // The first filter limits the rows to those that occurred in the year 2013.
      // This will also remove any header in the file.
      // The third and fourth columns contain the drop off latitude and longitude.
      // The transformation will throw exceptions if these values are empty.

      //Do another count to see what was removed.
      println(taxiData.count())

      // To fence the area roughly to New York City
      val taxiFence=taxiData.
        filter(_.split(",")(3).toDouble > 40.70).
        filter(_.split(",")(3).toDouble < 40.86).
        filter(_.split(",")(4).toDouble > (-74.02)).
        filter(_.split(",")(4).toDouble < (-73.93))

      //Determine how many are left in taxiFence:
      println(taxiFence.count())

      // Create Vectors with the latitudes and longitudes that will be used as input to the K-Means algorithm.
      val taxi = taxiFence.
        map{
          line => Vectors.dense(
            line.split(',').slice(3,5).map(_ .toDouble)
          )
        }

      val iterationCount = 10
      val clusterCount = 3

      val model = KMeans.train(taxi,clusterCount,iterationCount)
      val clusterCenters = model.clusterCenters.map(_.toArray)

      clusterCenters.foreach(lines => println(lines(0),lines(1)))

      // Now we know the map co-ordinates.
      // Not surprisingly, the second point is between the Theater District and Grand Central.
      // The third point is in The Village, NYU, Soho and Little Italy area.
      // The first point is the Upper East Side,
      // presumably where people are more likely to take cabs than subways.

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