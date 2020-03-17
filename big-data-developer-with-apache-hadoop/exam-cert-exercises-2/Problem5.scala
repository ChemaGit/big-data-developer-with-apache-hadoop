/**
  * Question 5: Correct
  * PreRequiste:
  * [PreRequiste will not be there in actual exam]
  *sqoop import \
  *--connect "jdbc:mysql://quickstart.cloudera/retail_db" \
  *--password cloudera \
  *--username root \
  *--table orders \
  *--fields-terminated-by "\t" \
  *--target-dir /user/cloudera/practice2/problem3/orders \
  *--outdir /home/cloudera/outdir \
  *--bindir /home/cloudera/bindir
  **
 sqoop import \
  *--connect "jdbc:mysql://quickstart.cloudera/retail_db" \
  *--password cloudera \
  *--username root \
  *--table order_items \
  *--fields-terminated-by "\t" \
  *--target-dir /user/cloudera/practice2/problem3/order_items \
  *--outdir /home/cloudera/outdir \
  *--bindir /home/cloudera/bindir
  **
 sqoop import \
  *--connect "jdbc:mysql://quickstart.cloudera/retail_db" \
  *--password cloudera \
  *--username root \
  *--table customers \
  *--fields-terminated-by "\t" \
  *--target-dir /user/cloudera/practice2/problem3/customers \
  *--outdir /home/cloudera/outdir \
  *--bindir /home/cloudera/bindir
  **
Instructions
Get all customers who have placed order of amount more than 200.

Input files are tab delimeted files placed at below HDFS location:
/user/cloudera/practice2/problem3/customers
/user/cloudera/practice2/problem3/orders
/user/cloudera/practice2/problem3/order_items

Schema for customers File
Customer_id,customer_fname,customer_lname,customer_email,customer_password,customer_street,customer_city,customer_state,customer_zipcode

Schema for Orders File
Order_id,order_date,order_customer_id,order_status

Schema for Order_Items File
Order_item_id,Order_item_order_id,order_item_product_id,Order_item_quantity,Order_item_subtotal,Order_item_product_price

Output Requirements:
>> Output should be placed in below HDFS Location
/user/cloudera/practice2/problem3/joinResults
>> Output file should be comma seperated file with customer_fname,customer_lname,customer_city,order_amount
*/

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object Problem5 {

  val spark = SparkSession
    .builder()
    .appName("Problem5")
    .master("local[*]")
    .enableHiveSupport()
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "Problem5")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val rootPath = "hdfs://quickstart.cloudera/user/cloudera/practice2/problem3/"

  def main(args: Array[String]): Unit = {

    try {
      Logger.getRootLogger.setLevel(Level.ERROR)

      val schemaOrders = StructType(List(StructField("order_id",IntegerType, false), StructField("order_date",StringType,false),
        StructField("order_customer_id",IntegerType,false),StructField("order_status", StringType)))
      val schemaOrderItems = StructType(List(StructField("item_id",IntegerType, false),
        StructField("item_order_id",IntegerType,false),StructField("item_product_id",IntegerType,false),
        StructField("item_quantity", IntegerType),StructField("item_subtotal", DoubleType), StructField("item_price", DoubleType)))
      val schemaCustomers = StructType(List(StructField("id", IntegerType, false),StructField("fname", StringType, false),StructField("lname", StringType,false),
        StructField("email",StringType, true),StructField("password", StringType,true), StructField("street", StringType, true),
        StructField("city",StringType,true),StructField("state", StringType,true),StructField("zipcode", StringType, true)))

      val orders = sqlContext
        .read
        .schema(schemaOrders)
        .option("sep","\t")
        .csv(s"${rootPath}orders")
        .cache()

      orders.show(5)

      val orderItems = sqlContext
        .read
        .schema(schemaOrderItems)
        .option("sep","\t")
        .csv(s"${rootPath}order_items")
        .cache()

      orderItems.show(5)

      val customers = sqlContext
        .read
        .schema(schemaCustomers)
        .option("sep", "\t")
        .csv(s"${rootPath}customers")
        .cache()

      customers.show(5)

      orders.createOrReplaceTempView("o")
      orderItems.createOrReplaceTempView("oi")
      customers.createOrReplaceTempView("c")

      // OUTPUT: customer_fname,customer_lname,customer_city,order_amount
      val data = sqlContext
        .sql(
          """SELECT fname, lname,city, ROUND(SUM(item_subtotal), 2) AS order_amount
            |FROM c JOIN o ON(c.id = o.order_customer_id) JOIN oi ON(o.order_id = oi.item_order_id)
            |GROUP BY fname, lname, city
          """.stripMargin)
        .cache()

      data.show(10)

      data
        .write
        .option("sep",",")
        .option("header", true)
        .csv(s"${rootPath}output")

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
--password cloudera \
--username root \
--table orders \
--fields-terminated-by "\t" \
--delete-target-dir \
--target-dir /user/cloudera/practice2/problem3/orders \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table order_items \
--fields-terminated-by "\t" \
--delete-target-dir \
--target-dir /user/cloudera/practice2/problem3/order_items \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table customers \
--fields-terminated-by "\t" \
--delete-target-dir \
--target-dir /user/cloudera/practice2/problem3/customers \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

val orders = sc.textFile("/user/cloudera/practice2/problem3/orders").map(line => line.split('\t')).map(r => (r(0).toInt,r(1),r(2).toInt,r(3))).toDF("order_id","order_date","order_customer_id","order_status")
val orderItems = sc.textFile("/user/cloudera/practice2/problem3/order_items").map(line => line.split('\t')).map(r => (r(0).toInt,r(1).toInt,r(2).toInt,r(3).toInt,r(4).toFloat,r(5).toFloat)).toDF("item_id","item_order_id","item_product_id","item_quantity","item_subtotal","item_product_price")
val customers = sc.textFile("/user/cloudera/practice2/problem3/customers").map(line => line.split('\t')).map(r => (r(0).toInt,r(1),r(2),r(3),r(4),r(5),r(6),r(7),r(8))).toDF("customer_id","customer_fname","customer_lname","customer_email","customer_password","customer_street","customer_city","customer_state","customer_zipcode")

orders.show(10)
orderItems.show(10)
customers.show(10)

orders.registerTempTable("o")
orderItems.registerTempTable("oi")
customers.registerTempTable("c")

val joined = sqlContext.sql("""SELECT customer_fname,customer_lname,customer_city,item_subtotal from c join o on(c.customer_id = o.order_customer_id) join oi on(o.order_id = oi.item_order_id)""")
joined.registerTempTable("joined")
val result = sqlContext.sql("""SELECT customer_fname,customer_lname,customer_city, round(sum(item_subtotal),2) as order_amount from joined group by customer_fname,customer_lname,customer_city order by order_amount""")
result.filter("order_amount > 200").rdd.map(r => r.mkString(",")).saveAsTextFile("/user/cloudera/practice2/problem3/joinResults")

$ hdfs dfs -ls /user/cloudera/practice2/problem3/joinResults
$ hdfs dfs -cat /user/cloudera/practice2/problem3/joinResults/part-00000 | head -n 20
*/