/**
Question 7: Correct
PreRequiste:
Import order_items and products into HDFS from mysql

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--username root \
--password cloudera \
--table order_items \
--target-dir /user/cloudera/practice4_ques6/order_items/ \
--delete-target-dir \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--username root \
--password cloudera \
--table products \
--target-dir /user/cloudera/practice4_ques6/products/ \
--delete-target-dir \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir

Instructions:
Find top 10 products which has made highest revenue.
Products and order_items data are placed in HDFS directory
/user/cloudera/practice4_ques6/order_items/ and /user/cloudera/practice4_ques6/products/ respectively.

Given Below case classes to be used:
case class Products(pId:Integer,name:String)
case class Orders(prodId:Integer,order_total:Float)

Data Description:
A mysql instance is running on the gateway node.In that instance you will find products table.
> Installation : on the cluser node gateway
> Database name: retail_db
> Table name: Products, Order_items
> Username: root
> Password: cloudera
Output Requirement:
Output should have product_id and revenue seperated with ':' and should be saved in /user/cloudera/practice4_ques6/output
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object problem7 {

  val spark = SparkSession
    .builder()
    .appName("problem7")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "problem7")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/practice4_ques6/"

  case class Products(pId:Integer,name:String)
  case class Orders(prodId:Integer,order_total:Float)

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      import spark.implicits._

      val products = sc
        .textFile(s"${path}products")
        .map(line => line.split(","))
        .map(r => Products(r(0).toInt,r(2)))
        .toDF

      val orders = sc
        .textFile(s"${path}order_items")
        .map(line => line.split(","))
        .map(r => Orders(r(1).toInt, r(4).toFloat))
        .toDF

      products.createOrReplaceTempView("p")
      orders.createOrReplaceTempView("o")

      val result = sqlContext
        .sql(
          """SELECT p.name, CONCAT(o.prodId, ":", ROUND(SUM(o.order_total),2)) AS total_revenue
            |FROM p JOIN o ON(p.pId = o.prodId)
            |GROUP BY p.name, o.prodId
            |ORDER BY total_revenue DESC
          """.stripMargin)
        .cache()

      result.show(10)

      result
        .write
        .mode("overwrite")
        .option("sep",",")
        .option("header", true)
        .csv(s"${path}output")

      //TODO: check the results
      // hdfs dfs -ls /user/cloudera/practice4_ques6/
      // hdfs dfs -ls /user/cloudera/practice4_ques6/output
      // hdfs dfs -cat /user/cloudera/practice4_ques6/output/*.csv | head -n 10

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
--table order_items \
--delete-target-dir \
--target-dir /user/cloudera/practice4_ques6/order_items/ \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--username root \
--password cloudera \
--table products \
--delete-target-dir \
--target-dir /user/cloudera/practice4_ques6/products/ \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

case class Products(pId:Integer,name:String)
case class Orders(prodId:Integer,order_total:Float)

val orderItems = sc.textFile("/user/cloudera/practice4_ques6/order_items/").map(line => line.split(",")).map(r => Orders(r(2).toInt,r(4).toFloat)).toDF
val products = sc.textFile("/user/cloudera/practice4_ques6/products/").map(line => line.split(",")).map(r => Products(r(0).toInt,r(2))).toDF

orderItems.registerTempTable("oi")
products.registerTempTable("p")
val result = sqlContext.sql("""SELECT pId, ROUND(SUM(order_total), 2) as total_revenue FROM p JOIN oi ON(p.pId = oi.prodId) GROUP BY pId ORDER BY total_revenue DESC LIMIT 10""")
result.rdd.map(r => r.mkString(":")).saveAsTextFile("/user/cloudera/practice4_ques6/output")

$ hdfs dfs -ls /user/cloudera/practice4_ques6/output
$ hdfs dfs -cat /user/cloudera/practice4_ques6/output/p*
*/