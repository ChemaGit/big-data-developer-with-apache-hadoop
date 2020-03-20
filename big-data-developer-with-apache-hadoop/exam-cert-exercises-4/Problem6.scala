/**
Question 6: Correct
PreRequiste:
[PreRequiste would not be there in final exam]

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table orders \
--fields-terminated-by "\t" \
--target-dir /user/cloudera/practice3/problem3/orders \
--delete-target-dir \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table order_items \
--fields-terminated-by "\t" \
--target-dir /user/cloudera/practice3/problem3/order_items \
--delete-target-dir \
--outdir /home/cloudera/outdir \Q
--bindir /home/cloudera/bindir

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table customers \
--fields-terminated-by "\t" \
--target-dir /user/cloudera/practice3/problem3/customers \
--delete-target-dir \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir


Instructions
Get count of customers in each city who have placed order of amount more than 100 and whose order status is not PENDING.

Input files are tab delimeted files placed at below HDFS location:
/user/cloudera/practice3/problem3/customers
/user/cloudera/practice3/problem3/orders
/user/cloudera/practice3/problem3/order_items

Schema for customers File
Customer_id,customer_fname,customer_lname,customer_email,customer_password,customer_street,customer_city,customer_state,customer_zipcode

Schema for Orders File
Order_id,order_date,order_customer_id,order_status

Schema for Order_Items File
Order_item_id,Order_item_order_id,order_item_product_id,Order_item_quantity,Order_item_subtotal,Order_item_product_price

Output Requirements:
Output should be placed in below HDFS Location
/user/cloudera/practice3/problem3/joinResults

Output file should be tab separated file with
[Providing the solution here only because answer is too long to put in choices.
You will not be provided with any answer choice in actual exam.Below answer is just provided to guide you]
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object Problem6 {

  val spark = SparkSession
    .builder()
    .appName("Problem6")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "Problem6")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/practice3/problem3/"

  //  Schema for customers File
  //  Customer_id,customer_fname,customer_lname,customer_email,customer_password,customer_street,customer_city,customer_state,customer_zipcode
  val customerSchema = StructType(List(StructField("customer_id", IntegerType, true), StructField("fname",StringType, true),
    StructField("lname", StringType, true), StructField("email",StringType, true),
    StructField("password",StringType, true), StructField("street",StringType, true),StructField("city",StringType, true),
    StructField("state",StringType, true),StructField("zipcode",StringType, true)))

  //  Schema for Orders File
  //  Order_id,order_date,order_customer_id,order_status
  val ordersSchema = StructType(List(StructField("order_id", IntegerType, true), StructField("order_date",StringType, true),
    StructField("order_customer_id", IntegerType, true),StructField("order_status", StringType, true)))

  //  Schema for Order_Items File
  //  Order_item_id,Order_item_order_id,order_item_product_id,Order_item_quantity,Order_item_subtotal,Order_item_product_price
  val orderItemsSchema = StructType(List(StructField("item_id", IntegerType, true), StructField("item_order_id",IntegerType, true),
    StructField("item_product_id",IntegerType, true),StructField("item_quantity",IntegerType, true),
    StructField("item_subtotal",DoubleType, true),StructField("item_product_price",DoubleType, true)))

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val orders = sqlContext
        .read
        .option("sep", "\t")
        .schema(ordersSchema)
        .csv(s"${path}orders")

      val orderItems = sqlContext
        .read
        .option("sep", "\t")
        .schema(orderItemsSchema)
        .csv(s"${path}order_items")

      val customers = sqlContext
        .read
        .option("sep", "\t")
        .schema(customerSchema)
        .csv(s"${path}customers")

      orders.show(5)
      orderItems.show(5)
      customers.show(5)

      orders.createOrReplaceTempView("o")
      orderItems.createOrReplaceTempView("oi")
      customers.createOrReplaceTempView("c")

      // Get count of customers in each city who have placed order of amount more than 100 and whose order status is not PENDING.
      val result = sqlContext
        .sql(
          """WITH otr AS (
            |SELECT item_order_id, SUM(item_subtotal) AS total_revenue
            |FROM oi
            |GROUP BY item_order_id
            |)
            |SELECT c.city, COUNT(c.customer_id) AS num_customers
            |FROM c JOIN o ON(c.customer_id = o.order_id) JOIN otr ON(o.order_id = otr.item_order_id)
            |WHERE o.order_status NOT LIKE("%PENDING%") AND
            |      otr.total_revenue > 100
            |GROUP BY c.city
            |ORDER BY num_customers DESC
          """.stripMargin)
        .cache()

      result.show(5)

      // Output should be placed in below HDFS Location
      // /user/cloudera/practice3/problem3/joinResults
      // Output file should be tab separated file
      result
        .write
        .mode("overwrite")      // TODO: check the results
      // hdfs dfs -ls /user/cloudera/practice3/problem3/
      // hdfs dfs -ls /user/cloudera/practice3/problem3/joinResults
      // hdfs dfs -cat /user/cloudera/practice3/problem3/joinResults/*.csv | head -n 10
        .option("sep","\t")
        .option("header", true)
        .csv(s"${path}joinResults")

      // TODO: check the results
      // hdfs dfs -ls /user/cloudera/practice3/problem3/
      // hdfs dfs -ls /user/cloudera/practice3/problem3/joinResults
      // hdfs dfs -cat /user/cloudera/practice3/problem3/joinResults/*.csv | head -n 10

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
--target-dir /user/cloudera/practice3/problem3/orders \
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
--target-dir /user/cloudera/practice3/problem3/order_items \
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
--target-dir /user/cloudera/practice3/problem3/customers \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

val orders = sc.textFile("/user/cloudera/practice3/problem3/orders").map(line => line.split('\t')).map(r => (r(0).toInt,r(1),r(2).toInt,r(3))).toDF("order_id","order_date","order_customer_id","order_status")
val orderItems = sc.textFile("/user/cloudera/practice3/problem3/order_items").map(line => line.split('\t')).map(r => (r(0).toInt,r(1).toInt,r(2).toInt,r(3).toInt,r(4).toFloat,r(5).toFloat)).toDF("item_id","item_order_id","order_item_product_id","item_quantity","item_subtotal","item_product_price")
val customers = sc.textFile("/user/cloudera/practice3/problem3/customers").map(line => line.split('\t')).map(r => (r(0).toInt,r(1),r(2),r(3),r(4),r(5),r(6),r(7),r(8))).toDF("customer_id","customer_fname","customer_lname","customer_email","customer_password","customer_street","customer_city","customer_state","customer_zipcode")

orders.registerTempTable("o")
orderItems.registerTempTable("oi")
customers.registerTempTable("c")

val joined = sqlContext.sql("""SELECT customer_city,customer_id,item_order_id, round(sum(item_subtotal),2) as total_revenue FROM c JOIN o ON(c.customer_id = o.order_customer_id) JOIN oi ON(o.order_id = oi.item_order_id) GROUP BY customer_city,customer_id,item_order_id""")

joined.registerTempTable("j")

val result = sqlContext.sql("""SELECT customer_city, count(customer_id) as total_customers from j where total_revenue > 100 group by customer_city""")

result.rdd.map(r => r.mkString("\t")).saveAsTextFile("/user/cloudera/practice3/problem3/joinResults")

$ hdfs dfs -ls /user/cloudera/practice3/problem3/joinResults
$ hdfs dfs -cat /user/cloudera/practice3/problem3/joinResults/p* | head -n 50
*/