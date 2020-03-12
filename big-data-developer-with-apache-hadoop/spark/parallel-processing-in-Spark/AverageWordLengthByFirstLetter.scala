/**
  * AverageWordLengthByFirstLetter
  * Example: For each letter, calculate the average length of the words starting with that letter
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object AverageWordLengthByFirstLetter {

  val spark = SparkSession
    .builder()
    .appName("AverageWordLengthByFirstLetter")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "AverageWordLengthByFirstLetter") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val myfile = "file:/home/training/training_materials/data/frostroad.txt"
      // specify 4 partitions to simulate a 4-block file
      val avglens = sc.textFile(myfile,4).
        flatMap(line => line.split("\\W")).
        filter(line => line.length > 0).
        map(word => (word(0),word.length)).
        groupByKey(2).
        map(pair => (pair._1, pair._2.sum/pair._2.size.toDouble))

      // call save to trigger the operations
      avglens.saveAsTextFile("/loudacre/avglen-output")

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