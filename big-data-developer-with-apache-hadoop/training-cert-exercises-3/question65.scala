/** Question 65
  * Problem Scenario 91 : You have been given data in json format as below.
  * {"first_name":"Ankit", "last_name":"Jain"}
  * {"first_name":"Amir", "last_name":"Khan"}
  * {"first_name":"Rajesh", "last_name":"Khanna"}
  * {"first_name":"Priynka", "last_name":"Chopra"}
  * {"first_name":"Kareena", "last_name":"Kapoor"}
  * {"first_name":"Lokesh", "last_name":"Yadav"}
  * Do the following activity
  * 1. create employee.json file locally.
  * 2. Load this file on hdfs
  * 3. Register this data as a temp table in Spark using scala.
  * 4. Write select query and print this data.
  * 5. Now save back this selected data as table in format orc. /user/cloudera/question65/orc
  * 6. Now save back this selected data as parquet file compressed in snappy codec in HDFS /user/cloudera/question65/parquet-snappy
  *
  * Build the file and put it in HDFS file system
  * $ gedit /home/cloudera/files/employee.json
  * $ hdfs dfs -put /home/cloudera/files/employee.json /user/cloudera/files/
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object question65 {

  val spark = SparkSession
    .builder()
    .appName("question65")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question65")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/files/"
  val outPath = "hdfs://quickstart.cloudera/user/cloudera/exercise_3/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val employee = spark
        .sqlContext
        .read
        .json(s"${path}employee.json")
        .cache()

      employee.createOrReplaceTempView("employee")

      sqlContext
        .sql(
          """SELECT first_name, last_name, CONCAT(first_name,", ", last_name) AS full_name
            |FROM employee""".stripMargin)
        .show()

      employee
        .write
        .orc(s"${outPath}orc")

      sqlContext
        .setConf("spark.sql.parquet.compression.codec","snappy")

      employee
        .write
        .parquet(s"${outPath}parquet-snappy")

      /**
        * Check the files
        * $ hdfs dfs -ls /user/cloudera/exercise_3/orc
        * $ hdfs dfs -ls /user/cloudera/exercise_3/parquet-snappy
        * $ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/question65/parquet-snappy/part-r-00000-9c6e9286-8d47-4274-9391-b26bdc66f3bb.snappy.parquet
        * $ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/question65/parquet-snappy/part-r-00000-9c6e9286-8d47-4274-9391-b26bdc66f3bb.snappy.parquet
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

val employee = sqlContext.read.json("/user/cloudera/files/employee.json")
employee.registerTempTable("employee")
sqlContext.sql("""select * from employee""").show()
employee.write.orc("/user/cloudera/question65/orc")
sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
employee.write.parquet("/user/cloudera/question65/parquet-snappy")

$ hdfs dfs -ls /user/cloudera/question65/orc
$ hdfs dfs -text /user/cloudera/question65/orc/part-r-00000-1321d325-f0e1-4742-9d5f-5cf5daebabf0.orc

$ hdfs dfs -ls /user/cloudera/question65/parquet-snappy
$ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/question65/parquet-snappy/part-r-00000-9c6e9286-8d47-4274-9391-b26bdc66f3bb.snappy.parquet
$ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/question65/parquet-snappy/part-r-00000-9c6e9286-8d47-4274-9391-b26bdc66f3bb.snappy.parquet
*/