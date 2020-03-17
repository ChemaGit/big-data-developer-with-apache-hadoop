/**
  * Question 6: Correct
  * Prerequisite:
  * [Prerequisite section will not be part of actual exam]
  * Import products table from mysql into hive metastore table named product_ranked in warehouse directory /public/retail_db
  * Run below sqoop statement
  **
 sqoop import \
  *--connect "jdbc:mysql://localhost/retail_db" \
  *--username root \
  *--password cloudera \
  *--table products \
  *--warehouse-dir /public/retail_db \
  *--hive-import \
  *--create-hive-table \
  *--hive-database hadoopexam \
  *--hive-table product_ranked -m 1
  **
 Instructions:
  * Provided a meta-store table named product_ranked consisting of product details ,find the most expensive product in each category.
  **
 Output Requirement:
  * Output should have product_category_id ,product_name,product_price,rank.
  * Result should be saved in /user/cloudera/pratice4/output/ as pipe delimited text file
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

  val output = "hdfs://quickstart.cloudera/user/cloudera/pratice4/output/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      sqlContext.sql("""SHOW databases""").show()
      sqlContext.sql(("USE hadoopexam"))
      sqlContext.sql("SELECT * FROM product_ranked LIMIT 10").show()

      /**
        * TODO: find the most expensive product in each category.
        * Output should have product_category_id ,product_name,product_price,rank.
        * Result should be saved in /user/cloudera/pratice4/output/ as pipe delimited text file
        */

      val rank = sqlContext.sql(
        """SELECT product_category_id, product_name,product_price, RANK() OVER(PARTITION BY product_category_id ORDER BY product_price DESC) as rank
          |FROM product_ranked
          |ORDER BY product_category_id, rank
        """.stripMargin)
        .cache

      val result_rank = rank
        .filter("rank = 1")

      result_rank.show(10)

      result_rank
        .write
        .option("sep","|")
        .option("header",false)
        .csv(output)

      // todo: check the results
      // hdfs dfs -ls /user/cloudera/pratice4/output/
      // hdfs dfs -cat /user/cloudera/pratice4/output/* | head -n 10

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
--table products \
--warehouse-dir /user/cloudera/practice4.db \
--hive-import \
--create-hive-table \
--hive-database default \
--hive-table product_ranked \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
-m 8

$ hive
hive> show tables;
hive> describe product_ranked;
hive> select * from product_ranked limit 10;

product_id          	int
product_category_id 	int
product_name        	string
product_description 	string
product_price       	double
product_image       	string

val rank = sqlContext.sql("""SELECT product_category_id, product_name,product_price, rank() over(partition by product_category_id order by product_price desc) as rank FROM product_ranked order by product_category_id, rank""")
val result = rank.filter("rank = 1")
result.rdd.map(r => r.mkString("|")).saveAsTextFile("/user/cloudera/pratice4/output/")

$ hdfs dfs -ls /user/cloudera/pratice4/output/
$ hdfs dfs -cat /user/cloudera/pratice4/output/part-00000 | head -n 50
*/