/** Question 94
  * Problem Scenario 78 : You have been given MySQL DB with following details.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * table=retail_db.orders
  * table=retail_db.order_items
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Columns of order table : (orderid , order_date , order_customer_id, order_status)
  * Columns of order_items table : (order_item_td , order_item_order_id , order_item_product_id, order_item_quantity,order_item_subtotal,order_item_product_price)
  * Please accomplish following activities.
  * 1. Copy "retail_db.orders" and "retail_db.order_items" table to hdfs in respective directory question94/orders and question94/order_items .
  * 2. Join these data using order_id in Spark and Scala
  * 3. Calculate total revenue perday and per customer
  * 4. Calculate maximum revenue customer
  *
  * $ sqoop import \
  * --connect jdbc:mysql://quickstart.cloudera/retail_db \
  * --username retail_dba \
  * --password cloudera \
  * --table orders \
  * --as-textfile \
  * --delete-target-dir \
  * --target-dir /user/cloudera/tables/orders \
  * --outdir /home/cloudera/outdir \
  * --bindir /home/cloudera/bindir
  *
  * sqoop import \
  * --connect jdbc:mysql://quickstart.cloudera/retail_db \
  * --username retail_dba \
  * --password cloudera \
  * --table order_items \
  * --as-textfile \
  * --delete-target-dir \
  * --target-dir /user/cloudera/tables/order_items \
  * --outdir /home/cloudera/outdir \
  * --bindir /home/cloudera/bindir
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object question94 {

  val spark = SparkSession
    .builder()
    .appName("question94")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question94")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/tables/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      // USING RDD
      val ordersRdd = sc
        .textFile(s"${path}orders/")
        .map(line => line.split(","))
        .map(arr => (arr(0).toInt, (arr(1), arr(2).toInt)))

      val orderItemsRdd = sc
        .textFile(s"${path}order_items/")
        .map(line => line.split(","))
        .map(arr => (arr(1).toInt, arr(4).toDouble))

      val joinedRdd = ordersRdd
        .join(orderItemsRdd)
        .cache()

      // 3. Calculate total revenue perday and per customer
      // (Int, ((String, Int), Double))

      val revenuePerdayCustomer = joinedRdd
        .map({case( (id,((date,cust), revenue)) ) => ( (date, cust), revenue)})
        .reduceByKey( (v, c) => v + c)
        .sortBy(t => t._2, false)

      revenuePerdayCustomer
        .take(25)
        .foreach(println)

      // 4. Calculate maximum revenue per customer
      val maxRevenueCustomer = joinedRdd
        .map({case( (id,((date,cust), revenue)) ) => (cust, revenue)})
        .reduceByKey( (v, c) => v + c)
        .sortBy(t => t._2, false)
        .first()

      println(s"Maximum revenue per customer: $maxRevenueCustomer")

      // USING DATAFRAMES

      val ordersSchema = StructType(List(StructField("order_id", IntegerType, false), StructField("order_date",StringType, false),
        StructField("order_customer_id", IntegerType, false), StructField("order_status",StringType, false)))
      //(order_item_td , order_item_order_id , order_item_product_id, order_item_quantity,order_item_subtotal,order_item_product_price)
      val orderItemsSchema = StructType(List(StructField("item_id", IntegerType, false), StructField("item_order_id",IntegerType, false),
        StructField("item_product_id", IntegerType, false), StructField("item_quantity",IntegerType, false),
        StructField("item_subtotal",DoubleType, false), StructField("item_price",DoubleType, false)))

      val ordersDF = sqlContext
        .read
        .schema(ordersSchema)
        .option("header", false)
        .option("sep",",")
        .csv(s"${path}orders/")
        .cache()

      val itemOrdersDF = sqlContext
        .read
        .schema(orderItemsSchema)
        .option("header", false)
        .option("sep",",")
        .csv(s"${path}order_items/")
        .cache()

      ordersDF.createOrReplaceTempView("orders")
      itemOrdersDF.createOrReplaceTempView("item_orders")

      // 3. Calculate total revenue perday and per customer
      sqlContext
        .sql(
          """SELECT order_date, order_customer_id, ROUND(SUM(item_subtotal),2) AS Total_Revenue
            |FROM orders JOIN item_orders ON(order_id = item_order_id)
            |GROUP BY order_date, order_customer_id
            |ORDER BY Total_Revenue DESC""".stripMargin)
        .show()

      // 4. Calculate maximum revenue per customer
      sqlContext
        .sql(
          """SELECT order_customer_id, ROUND(SUM(item_subtotal), 2) AS Max_Revenue_Customer
            |FROM orders JOIN item_orders ON(order_id = item_order_id)
            |GROUP BY order_customer_id
            |ORDER BY Max_Revenue_Customer DESC LIMIT 1""".stripMargin)
        .show()

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
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table orders \
  --delete-target-dir \
  --target-dir /user/cloudera/question94/orders \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table order_items \
  --delete-target-dir \
  --target-dir /user/cloudera/question94/order_items \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

val orders = sc.textFile("/user/cloudera/question94/orders").map(line => line.split(",")).map(r => (r(0).toInt,(r(1),r(2).toInt)))
val orderItems = sc.textFile("/user/cloudera/question94/order_items").map(line => line.split(",")).map(r => (r(1).toInt,r(4).toFloat))
val joined = orders.join(orderItems).map({case((id, ((date, cId), revenue))) => (date,cId,revenue)})
// SPARK RDD
// 3. Calculate total revenue perday and per customer
val agg = joined.map({case((date,cId,revenue)) => ( (date,cId),revenue)}).aggregateByKey(0.0F)( ( (i: Float,v: Float) => v + i), ( (c: Float, v: Float) => v + c) )
agg.collect.foreach(println)
// 4. Calculate maximum revenue customer
val maxCust = agg.map({case(( (date,cId),revenue)) => (cId,revenue)}).reduceByKey( (v,c) => v + c).sortBy(t => t._2, false).first
// maxCust: (Int, Float) = (791,10524.17)

// SPARK SQL
// 3. Calculate total revenue perday and per customer
val joinedDF = joined.toDF("date","cId","revenue")
joinedDF.registerTempTable("joined")
val addDF = sqlContext.sql("""SELECT DATE_FORMAT(date,"dd-MM-yyyy") AS date, cId, ROUND(SUM(revenue),2) AS sum_revenue FROM joined GROUP BY date,cId""")
// 4. Calculate maximum revenue customer
val maxCust = sqlContext.sql("""SELECT cId, ROUND(SUM(revenue),2) AS max_revenue FROM joined GROUP BY cId ORDER BY max_revenue DESC LIMIT 1""")
maxCust.show()

+---+-----------+
|cId|max_revenue|
+---+-----------+
|791|   10524.17|
+---+-----------+
*/
