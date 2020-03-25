package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object JoiningRDDs {

  val spark = SparkSession
    .builder()
    .appName("JoiningRDDs")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "JoiningRDDs")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val input = "hdfs://quickstart.cloudera/public/cognitive_class/"
  val output = "hdfs://quickstart.cloudera/user/cloudera/cognitive_class/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val readmeFile = sc.textFile(s"${input}readme.md")
      val readme1File = sc.textFile(s"${input}readme1.md")

      // How many Spark keywords are in each file?
      println(readmeFile.filter(line => line.contains("Spark")).count())
      println(readme1File.filter(line => line.contains("Spark")).count())

      // Now do a WordCount on each RDD so that the results are (K,V) pairs of (word,count)
      val readmeCount = readmeFile.
        flatMap(line => line.split(" ")).
        map(word => (word, 1)).
        reduceByKey(_ + _)

      val readme1Count = readme1File.
        flatMap(line => line.split(" ")).
        map(word => (word, 1)).
        reduceByKey(_ + _)

      println("Readme Count\n")
      readmeCount.collect() foreach println

      println("Readme1 Count\n")
      readme1Count.collect() foreach println

      // Now let's join these two RDDs together to get a collective set.
      // The join function combines the two datasets (K,V) and (K,W) together and get (K, (V,W)).
      // Let's join these two counts together and then cache it.
      val joined = readmeCount.join(readme1Count)
      joined.cache()

      joined.collect.foreach(println)

      // Let's combine the values together to get the total count.
      val joinedSum = joined.map(k => (k._1, (k._2)._1 + (k._2)._2))
      joinedSum.collect() foreach println

      // o check if it is correct, print the first five elements from the joined and the joinedSum RDD
      println("Joined Individial\n")
      joined.take(5).foreach(println)

      println("\n\nJoined Sum\n")
      joinedSum.take(5).foreach(println)

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