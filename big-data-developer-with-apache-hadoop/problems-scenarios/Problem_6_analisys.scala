/**
  * /**
  * Problem 6: Provide two solutions for steps 2 to 7
  * Using HIVE QL over Hive Context
  * Using Spark SQL over Spark SQL Context or by using RDDs
  * 1. create a hive meta store database named problem6 and import all tables from mysql retail_db database into hive meta store.
  * 2. On spark shell use data available on meta store as source and perform step 3,4,5 and 6. [this proves your ability to use meta store as a source]
  * 3. Rank products within department by price and order by department ascending and rank descending [this proves you can produce ranked and sorted data on joined data sets]
  * 4. find top 10 customers with most unique product purchases. if more than one customer has the same number of product purchases then the customer with the lowest customer_id will take precedence [this proves you can produce aggregate statistics on joined datasets]
  * 5. On dataset from step 3, apply filter such that only products less than 100 are extracted [this proves you can use subqueries and also filter data]
  * 6. On dataset from step 4, extract details of products purchased by top 10 customers which are priced at less than 100 USD per unit [this proves you can use subqueries and also filter data]
  * 7. Store the result of 5 and 6 in new meta store tables within hive. [this proves your ability to use metastore as a sink]
  **/
  *
  * 1. create a hive meta store database named problem6 and import all tables from mysql retail_db database into hive meta store.
  * $ beeline -u jdbc:hive2://quickstart.cloudera:10000
  * hive> CREATE DATABASE problem6;
  *
$ sqoop import-all-tables \
    --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
    --username retail_dba \
    --password cloudera \
    --as-textfile \
    --hive-import \
    --hive-database problem6 \
    --create-hive-table \
    --outdir /home/cloudera/outdir \
    --bindir /home/cloudera/bindir \
    --autoreset-to-one-mapper
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Problem_6_analisys {

  val spark = SparkSession
    .builder()
    .appName("Problem_6_analisys")
    .master("local[*]")
    .enableHiveSupport()
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "Problem_6_analisys")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      // 2. On spark shell use data available on meta store as source and perform step 3,4,5 and 6. [this proves your ability to use meta store as a source]
      sqlContext.sql("CREATE DATABASE problem6")
      sqlContext.sql("SHOW DATABASES").show()
      sqlContext.sql("""USE problem6""")
      // 3. Rank products within department by price and order by department ascending and rank descending [this proves you can produce ranked and sorted data on joined data sets]
      val hiveResult = sqlContext
        .sql(
          """SELECT d.department_id, p.product_id,p.product_name,p.product_price,
            |RANK() OVER(PARTITION BY d.department_id ORDER BY p.product_price) AS product_price_rank,
            |DENSE_RANK() OVER(PARTITION BY d.department_id ORDER BY p.product_price) AS product_dense_price_rank
            |FROM products p INNER JOIN categories c ON(c.category_id = p.product_category_id) INNER JOIN departments d ON(c.category_department_id = d.department_id)
            |ORDER BY d.department_id, product_price_rank DESC,product_dense_price_rank
          """.stripMargin)
        .cache()
      hiveResult.show()

      // 4. find top 10 customers with most unique product purchases. if more than one customer has the same number of product purchases then the
      // customer with the lowest customer_id will take precedence [this proves you can produce aggregate statistics on joined datasets]
      val hiveResult2 = sqlContext
        .sql(
          """SELECT c.customer_id, c.customer_fname, COUNT(DISTINCT(oi.order_item_product_id)) AS unique_product
            |FROM customers c INNER JOIN orders o ON(c.customer_id = o.order_customer_id) INNER JOIN order_items oi ON(o.order_id = oi.order_item_order_id)
            |GROUP BY c.customer_id, c.customer_fname,
            |ORDER BY unique_products DESC, c.customer_id
            |LIMIT 10
          """.stripMargin)
        .cache()
      hiveResult2.show()

      // 5. On dataset from step 3, apply filter such that only products less than 100 are extracted [this proves you can use subqueries and also filter data]
      val hiveResult3 = hiveResult
        .where("product_price < 100")
        .cache()

      hiveResult3.show()

      // 6. On dataset from step 4, extract details of products purchased by top 10 customers which are priced at less than 100 USD per unit [this proves you can use subqueries and also filter data]
      hiveResult2.createOrReplaceTempView("dataset4")
      val hiveResult4 = sqlContext
        .sql(
          """SELECT DISTINCT(p.*)
            |FROM products p JOIN order_items oi ON(oi.order_item_product_id = p.product_id) JOIN orders o ON(o.order_id = oi.order_item_order_id) JOIN dataset4 dt4 ON(o.order_customer_id = dt4.customer_id)
            |WHERE p.product_price < 100
          """.stripMargin)
        .cache()

      // 7. Store the result of 5 and 6 in new meta store tables within hive. [this proves your ability to use metastore as a sink]
      val path1 = "hdfs://quickstart.cloudera/user/hive/warehouse/problem6.db/hive_result_3"
      val path2 = "hdfs://quickstart.cloudera/user/hive/warehouse/problem6.db/hive_result_4"

      hiveResult3
        .write
        .parquet(path1)

      hiveResult4
        .write
        .parquet(path2)

      sqlContext
        .sql(
          s"""CREATE TABLE hive_result_3(Logger.getRootLogger.setLevel(Level.ERROR)
             |product_id INT,
             |product_name STRING,
             |product_price DOUBLE,
             |product_price_rank INT,
             |product_dense_rank INT)
             |STORED AS PARQUET
             |LOCATION "$path1" """.stripMargin)
      sqlContext.sql("""SELECT * FROM hive_result_3""").show(10)

      sqlContext
        .sql(
          s"""CREATE TABLE hive_result_4(
             |product_id INT,
             |product_category_id INT,
             |product_name STRING,
             |product_description STRING,
             |product_price DOUBLE,
             |product_image STRING)
             |STORED AS PARQUET
             |LOCATION "$path2" """.stripMargin)
      sqlContext.sql("""SELECT * FROM hive_result_4""").show(10)

      /*
      $ beeline -u jdbc:hive2://quickstart.cloudera:10000/problem6
      hive> show tables;
      hive> SELECT * FROM hive_result_3;
      hive> SELECT * FROM hive_result_4;
      hive> !q
       */


      // To have the opportunity to view the web console of Spark: http://localhost:4041/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("SparkContext stopped")
      spark.stop()
      println("SparkSession stopped")
    }

  }
}


/*SOLUTION IN THE SPARK REPL
// 1. create a hive meta store database named problem6 and import all tables from mysql retail_db database into hive meta store.
$ hive
hive> create database problem6;

sqoop import-all-tables \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username root \
--password cloudera \
--hive-import \
--hive-database problem6 \
--create-hive-table \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--autoreset-to-one-mapper

hive> use problem6;
hive> show tables;
hive> select * from orders limit 10;

// 2. On spark shell use data available on meta store as source and perform step 3,4,5 and 6. [this proves your ability to use meta store as a source]
sqlContext.sql("use problem6")
// 3. Rank products within department by price and order by department ascending and rank descending [this proves you can produce ranked and sorted data on joined data sets]
var hiveResult = sqlContext.sql("""SELECT d.department_id,p.product_id,p.product_name,p.product_price,rank() over(partition by d.department_id order by p.product_price) as product_price_rank,dense_rank() over(partition by d.department_id order by p.product_price) as product_dense_price_rank FROM products p INNER JOIN categories c ON(c.category_id = p.product_category_id) INNER JOIN departments d ON(c.category_department_id = d.department_id) ORDER BY d.department_id,product_price_rank desc,product_dense_price_rank """);

// 4. find top 10 customers with most unique product purchases. if more than one customer has the same number of product purchases then the customer with the lowest customer_id will take precedence [this proves you can produce aggregate statistics on joined datasets]
val hiveResult2 = sqlContext.sql("""SELECT c.customer_id,c.customer_fname,COUNT(DISTINCT(oi.order_item_product_id)) AS unique_products FROM customers c INNER JOIN orders o ON(c.customer_id = o.order_customer_id) INNER JOIN order_items oi ON(o.order_id = oi.order_item_order_id) GROUP BY c.customer_id,c.customer_fname ORDER BY unique_products DESC, c.customer_id LIMIT 10""")

// 5. On dataset from step 3, apply filter such that only products less than 100 are extracted [this proves you can use subqueries and also filter data]
hiveResult.registerTempTable("dataset3")
val hiveResult3 = sqlContext.sql("""SELECT * FROM dataset3 WHERE product_price < 100""")

// 6. On dataset from step 4, extract details of products purchased by top 10 customers which are priced at less than 100 USD per unit [this proves you can use subqueries and also filter data]
hiveResult2.registerTempTable("dataset4")
val hiveResult4 = sqlContext.sql("""SELECT DISTINCT p.* FROM products p JOIN order_items oi ON(oi.order_item_product_id = p.product_id) JOIN orders o ON(o.order_id = oi.order_item_order_id) JOIN dataset4 dt4 ON(o.order_customer_id = dt4.customer_id) WHERE p.product_price < 100""")
*/