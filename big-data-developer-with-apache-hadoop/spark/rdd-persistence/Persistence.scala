/**
 * RDD Persistence
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Persistence {

  val spark = SparkSession
    .builder()
    .appName("Persistence")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "Persistence") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val mydata = sc.textFile("file:/home/training/training_materials/devsh/examples/example-data/purplecow.txt")
      val myrdd1 = mydata.map(s => s.toUpperCase())
      myrdd1.toDebugString
      myrdd1.count()
      myrdd1.persist()
      val myrdd2 = myrdd1.filter(s => s.startsWith("I"))
      myrdd2.count()
      myrdd2.toDebugString

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
