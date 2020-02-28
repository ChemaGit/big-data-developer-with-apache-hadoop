import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

/** Question 32
  * Problem Scenario 39 : You have been given two files
  * spark16/file1.txt
  * 1,9,5
  * 2,7,4
  * 3,8,3
  * spark16/file2.txt
  * 1,g,h
  * 2,i,j
  * 3,k,l
  * Load these two tiles as Spark RDD and join them to produce the below results
  * (1,((9,5),(g,h))) (2, ((7,4), (i,j))) (3, ((8,3), (k,l)))
  * And write code snippet which will sum the second columns of above joined results (5+4+3).
  */

object question32 {

  val spark = SparkSession
    .builder()
    .appName("question32")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question32")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/files/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val file1 = sc
        .textFile(s"${path}file11.txt")
        .map(line => line.split(","))
        .map(arr => (arr(0).toInt,(arr(1).toInt,arr(2).toInt)))

      val file2 = sc
        .textFile(s"${path}file22.txt")
        .map(line => line.split(","))
        .map(arr => (arr(0).toInt,(arr(1),arr(2))))

      val joined = file1
        .join(file2)
        .cache()

      val result = joined
        .map({case( (k,((v,c),(m,n)))) => c})
        .reduce({case(c, c1) => c + c1})

      println(result)

      joined.unpersist()

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

/*SOLUTION IN THE SPARK REPL
val file1 = sc.textFile("/user/cloudera/files/file11.txt").map(line => line.split(",")).map(r => (r(0),(r(1),r(2).toInt)))
val file2 = sc.textFile("/user/cloudera/files/file22.txt").map(line => line.split(",")).map(r => (r(0),(r(1),r(2))))
val joined = file1.join(file2)
val result = joined.map({case((_,((_,v),(_,_)))) => v}).reduce( (v,c) => v + c)
*/