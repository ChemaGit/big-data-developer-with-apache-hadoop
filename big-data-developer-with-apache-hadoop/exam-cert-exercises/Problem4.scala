/**
  * Question 4: Correct
  * Prerequiste:
  * [Prerequisite section will not be there in actual exam]
  *
  * Import orders table from mysql into hdfs location /user/cloudera/practice4/orders/.Run below sqoop statement
  * Import customers from mysql into hdfs location /user/cloudera/practice4/customers/.Run below sqoop statement
  *
  * Instructions:
  *
  * Join the data at hdfs location /user/cloudera/practice4/orders/ & /user/cloudera/practice4/customers/ to find out customers whose orders status is like "pending"
  * Schema for customer File
  * Customer_id,customer_fname,customer_lname
  * Schema for Order File
  * Order_id,order_date,order_customer_id,order_status
  *
  * Output Requirement:
  * Output should have customer_id,customer_fname,order_id and order_status.Result should be saved in /user/cloudera/practice4/output
  *
  *
  */

/*
sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders \
--as-textfile \
--target-dir /user/cloudera/practice4/orders \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table customers \
--as-textfile \
--target-dir /user/cloudera/practice4/customers \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Problem4 {

  val spark = SparkSession
    .builder()
    .appName("Problem4")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "Problem4")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  case class Orders(order_id: Int, order_date: String, order_customer_id: Int, order_status: String)
  case class Customers(customer_id: Int, customer_fname: String, customer_lname: String, customer_city: String, customer_state: String)

  val rootPath = "hdfs://quickstart.cloudera/user/cloudera/practice4/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      import spark.implicits._

      val orders = sc
        .textFile(s"${rootPath}orders")
        .map(line => line.split(","))
        .map(r => Orders(r(0).toInt,r(1),r(2).toInt,r(3)))
        .toDF
        .cache
      orders.show(10)

      val customers = sc
        .textFile(s"${rootPath}customers")
        .map(line => line.split(","))
        .map(r => Customers(r(0).toInt,r(1),r(2),r(6),r(7)))
        .toDF
        .cache
      customers.show(10)

      orders.createOrReplaceTempView("orders")
      customers.createOrReplaceTempView("customers")

      val output = sqlContext
        .sql(
          """SELECT c.customer_id,c.customer_fname,o.order_id,o.order_status
            |FROM orders o
            |JOIN customers c ON(o.order_customer_id = c.customer_id)
            |WHERE o.order_status LIKE("%PENDING%")
            |ORDER BY c.customer_id
          """.stripMargin)
        .cache

      output.show(10)

      output
        .write
        .option("header",true)
        .csv(s"${rootPath}output")

      // To have the opportunity to view the web console of Spark: http://localhost:4041/
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
--table orders \
--delete-target-dir \
--target-dir /user/cloudera/practice4/question3/orders/ \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--username root \
--password cloudera \
--table customers \
--delete-target-dir \
--target-dir /user/cloudera/practice4/question3/customers/ \
--columns "customer_id,customer_fname,customer_lname" \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

val orders = sc.textFile("/user/cloudera/practice4/question3/orders/").map(line => line.split(",")).map(arr => (arr(0).toInt,arr(1),arr(2).toInt,arr(3))).toDF("order_id","order_date","order_customer_id","order_status")
val customer = sc.textFile("/user/cloudera/practice4/question3/customers/").map(line => line.split(",")).map(arr => (arr(0),arr(1),arr(2))).toDF("customer_id","customer_fname","customer_lname")

orders.registerTempTable("o")
customer.registerTempTable("c")

sqlContext.sql("""select customer_id,customer_fname,order_id,order_status from c join o on(c.customer_id = o.order_customer_id) where order_status like("%PENDING%")""").show(10)
val result = sqlContext.sql("""select customer_id,customer_fname,order_id,order_status from c join o on(c.customer_id = o.order_customer_id) where order_status like("%PENDING%")""")

result.rdd.map(r => r.mkString(",")).saveAsTextFile("/user/cloudera/p1/q7/output")

$ hdfs dfs -ls /user/cloudera/p1/q7/output
$ hdfs dfs -cat /user/cloudera/p1/q7/output/part-00000 | head -n 20
*/