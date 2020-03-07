/** Question 97
	* Use retail_db data set
	*
	* Problem Statement
	* Get daily revenue by product considering completed and closed orders.
	* Data need to be sorted by ascending order by date and descending order
	* by revenue computed for each product for each day.
	* Data for orders and order_items is available in HDFS /public/retail_db/orders and /public/retail_db/order_items
	* Data for products is available under /public/retail_db/products
	* Final output need to be stored under
	* HDFS location-avro format /user/cloudera/question97/daily_revenue_avro
	* HDFS location-text format /user/cloudera/question97/daily_revenue_txt
	* Local location /home/cloudera/daily_revenue_scala
	* Solution need to be stored under /home/cloudera/daily_revenue_scala
	*/

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/*
sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
--username root \
--password cloudera \
--table orders \
--as-textfile \
--target-dir /public/retail_db/orders \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir

sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
--username root \
--password cloudera \
--table order_items \
--as-textfile \
--target-dir /public/retail_db/order_items \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir

sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
--username root \
--password cloudera \
--table products \
--as-textfile \
--target-dir /public/retail_db/products \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir
 */

object question97 {

	val spark = SparkSession
		.builder()
		.appName("question97")
		.master("local[*]")
		.config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
		.config("spark.app.id", "question97")  // To silence Metrics warning
		.getOrCreate()

	val sc = spark.sparkContext

	case class Orders(order_id: Int, order_date: String)
	case class OrderItems(order_item_order_id: Int, order_item_product_id: Int, order_item_subtotal: Double)
	case class Products(product_id: Int, product_name: String)

	val bList = sc.broadcast(List("COMPLETE", "CLOSED"))

	val input = "hdfs://quickstart.cloudera/public/retail_db/"
	val output = "hdfs://quickstart.cloudera/user/cloudera/question97/"

	def main(args: Array[String]): Unit = {
		try {

			Logger.getRootLogger.setLevel(Level.ERROR)

			import spark.implicits._

			val ordersDF = sc
				.textFile(s"${input}orders")
				.map(line => line.split(","))
				.filter(r => bList.value.contains(r(3)))
				.map(arr => Orders(arr(0).toInt, arr(1).substring(0, 10)))
				.toDF()
				.cache()

			val orderItemsDF = sc
				.textFile(s"${input}order_items")
				.map(line => line.split(","))
				.map(arr => OrderItems(arr(1).toInt, arr(2).toInt, arr(4).toDouble))
				.toDF()
				.cache()

			val productsDF = sc
				.textFile(s"${input}products")
				.map(line => line.split(","))
				.map(arr => Products(arr(0).toInt, arr(2)))
				.toDF()
				.cache()

			ordersDF.createOrReplaceTempView("orders")
			orderItemsDF.createOrReplaceTempView("order_items")
			productsDF.createOrReplaceTempView("products")

			val result = spark
				.sqlContext
				.sql(
					"""SELECT product_id, product_name,order_date,ROUND(SUM(order_item_subtotal),2) AS daily_revenue
						|FROM orders JOIN order_items ON(order_id = order_item_order_id) JOIN products ON(order_item_product_id = product_id)
						| GROUP BY product_id,product_name,order_date
						| ORDER BY order_date ASC,daily_revenue DESC """.stripMargin)
				.cache()

			result.show(10)

			/*Final output need to be stored under
      HDFS location-avro format /user/cloudera/question97/daily_revenue_avro
      HDFS location-text format /user/cloudera/question97/daily_revenue_txt
     */

			import com.databricks.spark.avro._
			result
				.write
				.avro(s"${output}daily_revenue_avro")

			result
				.write
				.option("sep",",")
				.option("header", false)
				.csv(s"${output}daily_revenue_txt")

			println("Type whatever to the console to exit......")
			scala.io.StdIn.readLine()
		} finally {
			sc.stop()
			println("Stopped SparkContext")
			spark.stop()
			println("Stopped SparkSession")
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
  --target-dir /user/cloudera/question97/orders \
  --outdir /home/cloudera/outdir \
--outdir /home/cloudera/bindir \
--num-mappers 8

+-------------------+-------------+------+-----+---------+----------------+
| Field             | Type        | Null | Key | Default | Extra          |
+-------------------+-------------+------+-----+---------+----------------+
| order_id          | int(11)     | NO   | PRI | NULL    | auto_increment |
  | order_date        | datetime    | NO   |     | NULL    |                |
| order_customer_id | int(11)     | NO   |     | NULL    |                |
  | order_status      | varchar(45) | NO   |     | NULL    |                |
  +-------------------+-------------+------+-----+---------+----------------+

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table order_items \
  --delete-target-dir \
  --target-dir /user/cloudera/question97/order_items \
  --outdir /home/cloudera/outdir \
--outdir /home/cloudera/bindir \
--num-mappers 8

+--------------------------+------------+------+-----+---------+----------------+
| Field                    | Type       | Null | Key | Default | Extra          |
+--------------------------+------------+------+-----+---------+----------------+
| order_item_id            | int(11)    | NO   | PRI | NULL    | auto_increment |
  | order_item_order_id      | int(11)    | NO   |     | NULL    |                |
  | order_item_product_id    | int(11)    | NO   |     | NULL    |                |
  | order_item_quantity      | tinyint(4) | NO   |     | NULL    |                |
  | order_item_subtotal      | float      | NO   |     | NULL    |                |
| order_item_product_price | float      | NO   |     | NULL    |                |
+--------------------------+------------+------+-----+---------+----------------+

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table products \
  --delete-target-dir \
  --target-dir /user/cloudera/question97/products \
  --outdir /home/cloudera/outdir \
--outdir /home/cloudera/bindir \
--num-mappers 8

+---------------------+--------------+------+-----+---------+----------------+
| Field               | Type         | Null | Key | Default | Extra          |
+---------------------+--------------+------+-----+---------+----------------+
| product_id          | int(11)      | NO   | PRI | NULL    | auto_increment |
  | product_category_id | int(11)      | NO   |     | NULL    |                |
  | product_name        | varchar(45)  | NO   |     | NULL    |                |
  | product_description | varchar(255) | NO   |     | NULL    |                |
  | product_price       | float        | NO   |     | NULL    |                |
| product_image       | varchar(255) | NO   |     | NULL    |                |
  +---------------------+--------------+------+-----+---------+----------------+

val filt = List(""," ")
val orders = sc.textFile("/user/cloudera/question97/orders").map(line => line.split(",")).map(r => (r(0).toInt,r(1),r(2).toInt,r(3))).toDF("o_id","date","customer_id","status")
val orderItems = sc.textFile("/user/cloudera/question97/order_items").map(line => line.split(",")).map(r => (r(0).toInt,r(1).toInt,r(2).toInt,r(3).toInt,r(4).toFloat,r(5).toFloat)).toDF("oi_id","order_id","product_id","quantity","subtotal","product_price")
val products = sc.textFile("/user/cloudera/question97/products").map(line => line.split(",")).filter(r => !filt.contains(r(4))).map(r => (r(0).toInt,r(1).toInt,r(2),r(3),r(4).toFloat,r(5))).toDF("p_id","category_id","name","descr","price","image")
orders.registerTempTable("o")
orderItems.registerTempTable("oi")
products.registerTempTable("pr")

// Get daily revenue by product considering completed and closed orders.
// Data need to be sorted by ascending order by date and descending order
// by revenue computed for each product for each day
val joined = sqlContext.sql("""SELECT DATE_FORMAT(date, "dd-MM-yyyy") AS date, status, name, subtotal FROM o JOIN oi on(o_id = order_id) JOIN pr ON(product_id = p_id) WHERE status IN("COMPLETE","CLOSED")""")
joined.show()
joined.registerTempTable("joined")
val result = sqlContext.sql("""SELECT date,name,ROUND(SUM(subtotal),2) AS daily_revenue FROM joined GROUP BY date,name ORDER BY date ASC, daily_revenue DESC""")
result.show()
// HDFS location-avro format /user/cloudera/question97/daily_revenue_avro_scala
import com.databricks.spark.avro._
result.write.avro("/user/cloudera/question97/daily_revenue_avro_scala")
// HDFS location-text format /user/cloudera/question97/daily_revenue_txt_scala
result.rdd.map(r => r.mkString(",")).saveAsTextFile("/user/cloudera/question97/daily_revenue_txt_scala")

$ hdfs dfs -ls /user/cloudera/question97/daily_revenue_avro_scala
$ hdfs dfs -text /user/cloudera/question97/daily_revenue_avro_scala/part-r-00000-bd34e6ad-39f9-44e0-b677-162031ccfc41.avro
$ hdfs dfs -cat /user/cloudera/question97/daily_revenue_txt_scala/part* | head -n 50
*/