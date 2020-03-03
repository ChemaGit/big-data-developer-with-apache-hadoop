/** Question 51
  * Problem Scenario 72 : You have been given a table named "employee2" with following detail.
  * first_name string
  * last_name string
  * Write a spark script in scala which read this table and print all the rows and individual column values.
  * employee.json
  * {"first_name":"Ankit", "last_name":"Jain"}
  * {"first_name":"Amir", "last_name":"Khan"}
  * {"first_name":"Rajesh", "last_name":"Khanna"}
  * {"first_name":"Priynka", "last_name":"Chopra"}
  * {"first_name":"Kareena", "last_name":"Kapoor"}
  * {"first_name":"Lokesh", "last_name":"Yadav"}
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object question51 {

  // edit the file
  // $ gedit employee.json
  // put the file in hadoop file system
  // $ hdfs dfs -put employee.json /user/cloudera/files/

  val spark = SparkSession
    .builder()
    .appName("question51")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question51")  // To silence Metrics warning
    .enableHiveSupport()
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/files/"

  val pathDB = "hdfs://quickstart.cloudera/user/cloudera/hadoopexam/"

  // file:/home/cloudera/IdeaProjects/apache-spark-2.0-scala/spark-warehouse/hadoopexam.db

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      // sc.getConf.getAll.foreach(println)

      // create database hadoopexam
      sqlContext.sql(s"""CREATE DATABASE IF NOT EXISTS hadoopexam""")

      sqlContext.sql("""SHOW databases""").show()
      sqlContext.sql("""SHOW tables""").show()
      sqlContext
        .sql("""DESCRIBE DATABASE EXTENDED hadoopexam""")
        .show(100, truncate = false)

      // read the file and create a table employee2 whit Spark
      sqlContext
        .read
        .json(s"${path}employee.json")
        .rdd
        .map(r => r.mkString(","))
        .saveAsTextFile(s"${pathDB}t_employee2")

      sqlContext.sql("""USE hadoopexam""")

      sqlContext.sql("""DROP TABLE IF EXISTS t_employee2""")

      sqlContext.sql(
        s"""CREATE EXTERNAL TABLE IF NOT EXISTS t_employee2(first_name string, last_name string)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
           |LOCATION "${pathDB}t_employee2" """.stripMargin)

      sqlContext
        .sql("""SHOW TABLES""")
        .show()

      sqlContext
        .sql("""DESCRIBE FORMATTED t_employee2""")
        .show(100, truncate = false)

      sqlContext.sql(
        """SELECT * FROM
          |t_employee2""".stripMargin)
        .show()

      sqlContext
        .sql("""DROP TABLE t_employee2""")

      sqlContext
        .sql("""SHOW TABLES""")
        .show()

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
// edit the file
$ gedit employee.json &
// put the file in hadoop file system
$ put /home/cloudera/files/employee.json /user/cloudera/files/

// read the file and create a table employee2 with Spark
val employee = sqlContext.read.json("/user/cloudera/files/employee.json")
employee.repartition(1).rdd.map(r => r.mkString(",")).saveAsTextFile("/user/hive/warehouse/hadoopexam.db/employee2")
sqlContext.sql("use hadoopexam")
sqlContext.sql("""create table employee2(first_name string, last_name string) row format delimited fields terminated by "," stored as textfile location "/user/hive/warehouse/hadoopexam.db/employee2" """)

sqlContext.sql("""select * from employee2""").show()

$ hive
hive> use hadoopexam;
hive> select * from employee2;
hive> describe formatted employee2;
hive> exit;
*/