/*
Question 2: Correct
Prerequistee:
Import products table from mysql into hive metastore table named product_ranked_new in warehouse directory /user/cloudera/practice5.db. Run below sqoop statement

sqoop import \
--connect "jdbc:mysql://gateway/retail_db" \
--username root \
--password cloudera \
--table products \
--warehouse-dir /user/cloudera/practice5.db \
--hive-import \
--create-hive-table \
--hive-database default \
--hive-table product_ranked_new \
-m 1

Instructions:
using product_ranked_new metastore table, Find the most expensive products within each category
Output Requirement:
Output should have product_id,product_name,product_price,product_category_id.Result should be saved in /user/cloudera/pratice4/question2/output
*/

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Problem2 {

  val spark = SparkSession
    .builder()
    .appName("Problem2")
    .master("local[*]")
    .enableHiveSupport()
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "Problem2")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val output = "hdfs://quickstart.cloudera/user/cloudera/pratice4/question2/output/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      sqlContext.sql("SHOW databases").show()
      sqlContext.sql("USE hadoopexam")
      sqlContext.sql("SHOW tables").show()
      sqlContext
        .sql(
          """CREATE EXTERNAL TABLE IF NOT EXISTS product_ranked_new(
            |product_id INT,
            |product_category_id INT,
            |product_name STRING,
            |product_description STRING,
            |product_price DOUBLE,
            |product_image STRING
            |) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
            |  STORED AS TEXTFILE
            |  LOCATION "hdfs://quickstart.cloudera/public/retail_db/products/"
          """.stripMargin)
      sqlContext.sql("SHOW tables").show()
      sqlContext.sql("""SELECT * FROM product_ranked_new LIMIT 10""").show()

      val result = sqlContext
        .sql(
          """WITH rnk AS (
            |SELECT product_id,product_name,product_price,product_category_id,
            |RANK() OVER (PARTITION BY product_category_id ORDER BY product_price DESC) AS rank
            |FROM product_ranked_new
            |ORDER BY product_category_id, rank
            |)
            |SELECT rnk.product_id,rnk.product_name,rnk.product_price,rnk.product_category_id
            |FROM rnk
            |WHERE rnk.rank = 1
          """.stripMargin)
        .cache()

      result
        .write
        .option("sep","\t")
        .option("header", true)
        .csv(output)

      // TODO: check the results
      // hdfs dfs -ls /user/cloudera/pratice4/question2/output/
      // hdfs dfs -cat /user/cloudera/pratice4/question2/output/* | head -n 10

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
--connect "jdbc:mysql://quickstart:3306/retail_db" \
--username root \
--password cloudera \
--table products \
--warehouse-dir /user/cloudera/practice5.db \
--hive-import \
--create-hive-table \
--hive-database default \
--hive-table product_ranked_new \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
-m 8

sqlContext.sql("use default")
val ranked = sqlContext.sql("""SELECT product_id,product_name,product_price,product_category_id, rank() over(partition by product_category_id order by product_price desc) as rank FROM product_ranked_new order by product_category_id desc, rank""")
ranked.registerTempTable("ranked")
val result = sqlContext.sql("""SELECT product_id,product_name,product_price,product_category_id FROM ranked WHERE rank = 1 order by product_category_id""")
result.rdd.map(r => r.mkString(",")).saveAsTextFile("/user/cloudera/pratice4/question2/output")

$ hdfs dfs -ls /user/cloudera/pratice4/question2/output
$ hdfs dfs -cat /user/cloudera/pratice4/question2/output/part* | head -n 50
*/