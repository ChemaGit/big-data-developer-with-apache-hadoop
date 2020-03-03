/** Question 57
  * Problem Scenario 73 : You have been given data in json format as below.
  * {"first_name":"Ankit", "last_name":"Jain"}
  * {"first_name":"Amir", "last_name":"Khan"}
  * {"first_name":"Rajesh", "last_name":"Khanna"}
  * {"first_name":"Priynka", "last_name":"Chopra"}
  * {"first_name":"Kareena", "last_name":"Kapoor"}
  * {"first_name":"Lokesh", "last_name":"Yadav"}
  * Do the following activity
  * 1. create employee.json file locally.
  * 2. Load this file on hdfs
  * 3. Register this data as a temp table in Spark using Scala.
  * 4. Write select query and print this data.
  * 5. Now save back this selected data in json format on the directory question57.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object question57 {

  val spark = SparkSession
    .builder()
    .appName("question57")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question57")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/files/"
  val output = "hdfs://quickstart.cloudera/user/cloudera/question_57"

  def main(args: Array[String]): Unit = {
    /**
      * build the file locally and put it in HDFS
      * $ gedit /home/cloudera/files/employee.json &
      * $ hdfs dfs -put /home/cloudera/files/employee.json /user/cloudera/files/
      * $ hdfs dfs -cat /user/cloudera/files/employee.json
      */

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val emp = sqlContext
        .read
        .json(s"${path}/employee.json")
        .cache()

      emp.createOrReplaceTempView("employee")

      sqlContext
        .sql(
          """SELECT date_format(current_date,'dd/MM/yyyy') AS date, first_name, last_name, concat(first_name,",",last_name)
            |FROM employee""".stripMargin)
        .show()

      val result = sqlContext
        .sql(
          """SELECT date_format(current_date,'dd/MM/yyyy') AS date, first_name, last_name, concat(first_name,",",last_name)
            |FROM employee""".stripMargin)

      result
        .toJSON
        .rdd
        .saveAsTextFile(output)

      emp.unpersist()

      /**
        * Check the results
        * $ hdfs dfs -ls /user/cloudera/question_57/
        * $ hdfs dfs -cat /user/cloudera/question_57/part-00000
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
$ gedit /home/cloudera/files/employee.json &
$ hdfs dfs -put /home/cloudera/files/employee.json /user/cloudera/files
$ hdfs dfs -cat /user/cloudera/files/employee.json

val emp = sqlContext.read.json("/user/cloudera/files/employee.json")
emp.registerTempTable("employee")
sqlContext.sql("""select date_format(current_date,'dd/MM/yyyy') as date, first_name, last_name, concat(first_name,",",last_name) as full_name from employee""").show()
val result = sqlContext.sql("""select date_format(current_date,'dd/MM/yyyy') as date, first_name, last_name, concat(first_name,",",last_name) as full_name from employee""")
result.toJSON.saveAsTextFile("/user/cloudera/question57/json")

$ hdfs dfs -ls /user/cloudera/question57/json
$ hdfs dfs -cat /user/cloudera/question57/json/part-00000
*/