import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object WordCount {
  val spark = SparkSession
    .builder()
    .appName("WordCount")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "WordCount")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/files/purplecow.txt"

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      // Typical example of Word Count using RDDs
      val countsRDD = sc.textFile(path).
        flatMap(line => line.split(" ")).
        map(word => (word,1)).
        reduceByKey((v1,v2) => v1+v2)

      // Word Count using Datasets instead
      val countsDS = sqlContext.read.text(filename).as[String].
        flatMap(line => line.split(" ")).
        groupBy(word => word).
        count()

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