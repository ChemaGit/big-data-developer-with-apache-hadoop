/** Question 78
  * Problem Scenario 35 : You have been given a file named spark7/EmployeeName.csv (id,name).
  * EmployeeName.csv
  * E01,Lokesh
  * E02,Bhupesh
  * E03,Amit
  * E04,Ratan
  * E05,Dinesh
  * E06,Pavan
  * E07,Tejas
  * E08,Sheela
  * E09,Kumar
  * E10,Venkat
  * 1. Load this file from hdfs and sort it by name and save it back as (id,name) in results directory. However, make sure while saving it should be able to write In a single file.
  *
  * $ gedit /home/cloudera/files/EmployeeName.csv
  * $ hdfs dfs -put /home/cloudera/files/EmployeeName.csv /user/cloudera/files/
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object question78 {

  val spark = SparkSession
    .builder()
    .appName("question78")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question78")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val output = "hdfs://quickstart.cloudera/user/cloudera/exercises/question_78/"
  val input = "hdfs://quickstart.cloudera/user/cloudera/files/EmployeeName.csv"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val emp = sc
        .textFile(input)
        .map(lines => lines.split(","))
        .map(r => (r(0), r(1)))
        .sortBy(t => t._2)

      emp.saveAsTextFile(output)

      /**
        * $ hdfs dfs -ls /user/cloudera/exercises/question_78/
        * $ hdfs dfs -cat /user/cloudera/exercises/question_78/part*
        */

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
$ gedit /home/cloudera/files/EmployeeName.csv
$ hdfs dfs -put /home/cloudera/files/EmployeeName.csv /user/cloudera/files

val emp = sc.textFile("/user/cloudera/files/EmployeeName.csv").map(line => line.split(",")).map(r => (r(0),r(1))).sortBy(t => t._2)
emp.map(t => "(%s,%s)".format(t._1,t._2)).saveAsTextFile("/user/cloudera/question78/result")

$ hdfs dfs -ls /user/cloudera/question78/result/
  $ hdfs dfs -cat /user/cloudera/question78/result/part-00000
*/