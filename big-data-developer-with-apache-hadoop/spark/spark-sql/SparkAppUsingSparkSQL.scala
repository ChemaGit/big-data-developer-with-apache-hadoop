package spark.spark-sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


/**
  * // download data from IBM Servier
  * // this may take ~30 seconds depending on your internet speed
  * //"wget --quiet https://cocl.us/BD0211EN_Data" !
  * //println("Data Downloaded!")
  * // unzip the folder's content into "resources" directory
  * //"unzip -q -o -d /resources/jupyterlab/labs/BD0211EN/ BD0211EN_Data" !
  * //println("Data Extracted!")
  */
object SparkAppUsingSparkSQL {

  val spark = SparkSession
    .builder()
    .appName("SparkAppUsingSparkSQL")
    .master("local[*]")
    .enableHiveSupport()
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "SparkAppUsingSparkSQL")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val input = "/home/cloudera/CognitiveClass/data/LabData/nycweather.csv"

  // Create a case class in Scala that defines the schema of the table.
  case class Weather(date: String, temp: Int, precipitation: Double)

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      // There are three columns in the dataset, the date, the mean temperature in Celsius,
      // and the precipitation for the day.
      // Since we already know the schema, we will infer the schema using reflection.
      val lines = scala.io.Source.fromFile(input).mkString
      println(lines)

      import spark.implicits._

      val weather = sc
        .textFile(input)
        .map(_.split(","))
        .map(w => Weather(w(0), w(1).trim.toInt, w(2).trim.toDouble))
        .toDF()

      // register the RDD as a table.
      weather.createOrReplaceTempView("weather")

      val hottest_with_precip = sqlContext.sql("SELECT * FROM weather WHERE precipitation > 0.0 ORDER BY temp DESC")

      hottest_with_precip.show(10)

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