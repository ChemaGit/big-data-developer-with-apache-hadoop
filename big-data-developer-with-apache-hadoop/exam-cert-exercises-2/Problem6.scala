/**
  * Question 6: Correct
  * PreRequiste:
  *[PreRequiste will not be there in actual exam]
  *Run below sqoop command to import customers table from mysql into hive table customers_hive:
  **
 sqoop import \
  *--connect "jdbc:mysql://localhost/retail_db" \
  *--username root \
  *--password cloudera \
  *--table customers \
  *--warehouse-dir /user/cloudera/problem3/customers_hive/input \
  *--hive-import \
  *--create-hive-table \
  *--hive-database default \
  *--hive-table customers_hive
  **
 Instructions:
  *Get Customers from metastore table named "customers_hive" whose fname is like "Rich" and save the results in HDFS in text format.
  **
 Output Requirement:
  *Result should be saved in /user/cloudera/practice2/problem4/customers/output as text file. Output should contain only fname, lname and city
  *fname and lname should seperated by tab with city seperated by colon
  **
 Sample Output
  *Richard Plaza:Francisco
  *Rich Smith:Chicago
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Problem6 {

  val spark = SparkSession
    .builder()
    .appName("Problem6")
    .master("local[*]")
    .enableHiveSupport()
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "Problem6")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val rootOutput = "hdfs://quickstart.cloudera/user/cloudera/practice2/problem4/customers/output"
  val rootInput = "hdfs://quickstart.cloudera/public/retail_db/customers"

  def main(args: Array[String]): Unit = {

    try {

      Logger.getRootLogger.setLevel(Level.ERROR)

      sqlContext
        .sql("""USE default""")

      sqlContext
        .sql(
          s"""CREATE EXTERNAL TABLE IF NOT EXISTS customers_hive(
             |id INT,
             |fname STRING,
             |lname STRING,
             |email STRING,
             |password STRING,
             |street STRING,
             |city STRING,
             |state STRING,
             |zipcode STRING
             |) ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
             |STORED AS TEXTFILE
             |LOCATION "$rootInput"
            """.stripMargin)

      val outputData = sqlContext
        .sql(
          """SELECT CONCAT(fname,"|",lname) AS full_name, city
            |FROM customers_hive
            |WHERE fname LIKE("%Rich%")
          """.stripMargin)
        .cache()

      outputData.show(10)

      outputData
        .write
        .option("sep",":")
        .option("header", true)
        .csv(rootOutput)

      // TODO: check the results
      // hdfs dfs -cat /user/cloudera/practice2/problem4/customers/output/*.csv | head -n 10

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
sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--username root \
--password cloudera \
--table customers \
--warehouse-dir /user/cloudera/problem3/customers_hive/input \
--hive-import \
--create-hive-table \
--hive-database default \
--hive-table customers_hive \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hive
hive> use default;
hive> show tables;
hive> select * from customers_hive limit 10;
hive> describe customers_hive;
hive> exit;

sqlContext.sql("use default")
val result = sqlContext.sql("""SELECT CONCAT(customer_fname,"\t",customer_lname,":",customer_city) as result FROM customers_hive WHERE customer_fname LIKE("%Rich%")""")
result.rdd.map(r => r.mkString("")).saveAsTextFile("/user/cloudera/practice2/problem4/customers/output")

$ hdfs dfs -ls /user/cloudera/practice2/problem4/customers/output
$ hdfs dfs -cat /user/cloudera/practice2/problem4/customers/output/par* | head -n 20
*/