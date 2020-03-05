/** Question 15
  * Problem Scenario 80 : You have been given MySQL DB with following details.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * table=retail_db.products
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Columns of products table : (product_id | product_category_id | product_name |
  * product_description | product_price | product_image )
  * Please accomplish following activities.
  * 1. Copy "retaildb.products" table to hdfs in a directory p93_products
  * 2. Now sort the products data sorted by product price per category, use productcategoryid
  * colunm to group by category
  */
// Previous steps
/*
sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table products \
--as-textfile \
--delete-target-dir \
--target-dir /public/cloudera/retail_db/products \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir

hdfs dfs -ls /user/cloudera/exercise_10/products
*/

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object question15{
  val spark = SparkSession
    .builder()
    .appName("question15")
    .master("local")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question15")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val path = "hdfs://quickstart.cloudera/public/cloudera/retail_db/products"

  val filt = sc.broadcast(List(""," "))

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val products = sc
        .textFile(path)
        .map(line => line.split(","))
        .filter(arr => filt.value.contains(arr(4)) == false)
        .map(arr => ( (arr(1).toInt, arr(4).toFloat),arr.mkString(",")))
        .cache()

      products
        .sortByKey()
        .collect
        .foreach({case(k,v) => println(v)})

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
val filt = List("", " ")
val products = sc.textFile("/user/cloudera/question15/products").map(line => line.split(",")).filter(r => !filt.contains(r(4))).map(r => ( (r(1).toInt,r(4).toFloat), r.mkString(",")))
val productsSorted = products.sortByKey(false)

productsSorted.collect.foreach(println)
*/