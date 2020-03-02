/** Question 46
  * Problem Scenario 74 : You have been given MySQL DB with following details.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * table=retail_db.orders
  * table=retail_db.order_items
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Columns of order table : (orderid , order_date , ordercustomerid, order_status}
  * Columns of order_items table : (order_item_td , order_item_order_id ,order_item_product_id, order_item_quantity,order_item_subtotal,order_item_product_price)
  * Please accomplish following activities.
  * 1. Copy "retaildb.orders" and "retaildb.order_items" table to hdfs in respective directory question46/orders and question46/order_items .
  * 2. Join these data using orderid in Spark and Scala
  * 3. Now fetch selected columns from joined data Orderid, Order_date and amount collected on this order.
  * 4. Calculate total order placed for each date, and produced the output sorted by date.
  */

/*
sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders \
--as-textfile \
--delete-target-dir \
--target-dir /user/cloudera/tables/orders \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table order_items \
--as-textfile \
--delete-target-dir \
--target-dir /user/cloudera/tables/order_items \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8
*/

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object question46 {

  val spark = SparkSession
    .builder()
    .appName("question46")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question46")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/tables/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      // SPARK-RDD SOLUTION
      val orders = sc
        .textFile(s"${path}orders")
        .map(line => line.split(","))
        .map(r => (r(0).toInt, r(1)))
        .cache()

      val orderItems = sc
        .textFile(s"${path}order_items")
        .map(line => line.split(","))
        .map(r => (r(1).toInt, r(4).toFloat))
        .cache()

      val joined = orders
        .join(orderItems)
        .map({case((id,(date,subtotal))) => ((id,date.substring(0,10)),subtotal)})
        .cache()

      val ordersPerDate = joined
        .groupByKey()
        .map({case(((id, date),iter)) => (date,1)}).reduceByKey((v,c) => v + c)
        .sortByKey()

      ordersPerDate
        .take(10)
        .foreach(println)

      joined.unpersist()

      println()
      println("***************")
      println()

      // SPARK-SQL SOLUTION
      import spark.implicits._

      val ordersDF = orders
        .toDF("id","date")
        .cache()
      orders.unpersist()

      val orderItemsDF = orderItems
        .toDF("id","subtotal")
        .cache()
      orderItems.unpersist()

      ordersDF.createOrReplaceTempView("o")
      orderItemsDF.createOrReplaceTempView("oi")

      val joinedDF = sqlContext
        .sql(
          """SELECT o.id, date, subtotal
            						|FROM o JOIN oi ON(o.id = oi.id) """.stripMargin)
        .cache()

      ordersDF.unpersist()
      orderItemsDF.unpersist()

      joinedDF.createOrReplaceTempView("j")

      val distinctIdDate = sqlContext
        .sql(
          """SELECT date, id
            						|FROM j  GROUP BY date, id """.stripMargin)
        .cache()

      joined.unpersist()

      distinctIdDate.createOrReplaceTempView("dd")

      sqlContext
        .sql(
          """SELECT substr(date, 0, 10) AS date, COUNT(id) AS total_orders
            						|FROM dd
            						|GROUP BY date
            						|ORDER BY date""".stripMargin)
        .show(10)

      distinctIdDate.unpersist()

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
  --as-textfile \
  --delete-target-dir \
  --target-dir /user/cloudera/question46/orders \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table order_items \
  --as-textfile \
  --delete-target-dir \
  --target-dir /user/cloudera/question46/order_items \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

// SPARK-RDD SOLUTION
val orders = sc.textFile("/user/cloudera/question46/orders").map(line => line.split(",")).map(r => (r(0).toInt,r(1)))
val orderItems = sc.textFile("/user/cloudera/question46/order_items").map(line => line.split(",")).map(r => (r(1).toInt,r(4).toFloat))

val joined = orders.join(orderItems).map({case( (id,(date,subtotal)) ) => ( (id,date.substring(0,10)),subtotal)})

val ordersPerDate = joined.groupByKey().map({case(((id, date), iter)) => (date, 1)}).reduceByKey( (v,c) => v + c).sortByKey()
ordersPerDate.take(10).foreach(println)

//SPARK-SQL SOLUTION
val orders = sc.textFile("/user/cloudera/question46/orders").map(line => line.split(",")).map(r => (r(0).toInt,r(1))).toDF("id","date")
val orderItems = sc.textFile("/user/cloudera/question46/order_items").map(line => line.split(",")).map(r => (r(1).toInt,r(4).toFloat)).toDF("id","subtotal")

orders.registerTempTable("o")
orderItems.registerTempTable("oi")
val joined = sqlContext.sql("""select o.id, date, subtotal from o join oi on(o.id = oi.id)""")
joined.registerTempTable("j")
val distinctIdDate = sqlContext.sql("""select date, id from j group by date, id""")
distinctIdDate.registerTempTable("dd")
val ordersPerDate = sqlContext.sql("""select substr(date,0,10) as date, count(id) as total_orders from dd group by date order by date""")
ordersPerDate.show(10)
*/