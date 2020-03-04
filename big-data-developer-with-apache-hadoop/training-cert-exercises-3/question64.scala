/** Question 64
  * Problem Scenario 34 : You have given a file named spark6/user.csv.
  * Data is given below:
  * user.csv
  * id,topic,hits
  * Rahul,scala,120
  * Nikita,spark,80
  * Mithun,spark,1
  * myself,cca175,180
  * Now write a Spark code in scala which will remove the header part and create RDD of values as below, for all rows. And also if id is myself" than filter out row.
  * Map(id -> om, topic -> scala, hits -> 120)
  */
/** Question 64
  * Problem Scenario 34 : You have given a file named spark6/user.csv.
  * Data is given below:
  * user.csv
  * id,topic,hits
  * Rahul,scala,120
  * Nikita,spark,80
  * Mithun,spark,1
  * myself,cca175,180
  * Now write a Spark code in scala which will remove the header part and create RDD of values as below, for all rows. And also if id is myself" than filter out row.
  * Map(id -> om, topic -> scala, hits -> 120)
  *
  * Build the file and put it in HDFS file system
  * $ gedit /home/cloudera/files/user.csv
  * $ hdfs dfs -put /home/cloudera/files/user.csv /user/cloudera/files
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object question64 {

  val spark = SparkSession
    .builder()
    .appName("question64")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question64")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val users = sc
        .textFile("hdfs://quickstart.cloudera/user/cloudera/files/user.csv")
        .map(line => line.split(","))

      val head = users.first()

      val wFilter = List(head(0), "myself")

      val userFiltered = users
        .filter(r => !wFilter.contains(r(0)))
        .map(r => r.zip(head).toMap)

      userFiltered
        .collect
        .foreach(println)

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
$ gedit /home/cloudera/files/user.csv &
$ hdfs dfs -put /home/cloudera/files/user.csv /user/cloudera/files

val user = sc.textFile("/user/cloudera/files/user.csv").map(lines => lines.split(","))
val header = user.first
val userFiltered = user.filter(r => r(0) != header(0)).filter(r => r(0) != "myself")
userFiltered.map(r => r.zip(header).toMap).collect
// res2: Array[scala.collection.immutable.Map[String,String]] = Array(Map(Rahul -> id, scala -> topic, 120 -> hits), Map(Nikita -> id, spark -> topic, 80 -> hits), Map(Mithun -> id, spark -> topic, 1 -> hits))
*/