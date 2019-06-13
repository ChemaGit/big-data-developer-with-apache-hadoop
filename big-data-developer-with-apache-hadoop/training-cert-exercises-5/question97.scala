/** Question 97
  * Use retail_db data set
  *
  * Problem Statement
  * Get daily revenue by product considering completed and closed orders.
  * Data need to be sorted by ascending order by date and descending order
	  by revenue computed for each product for each day.
  * Data for orders and order_items is available in HDFS /user/cloudera/question97/order_items
  * /user/cloudera/question97/orders
  * Data for products is available under /user/cloudera/question97/products
  * Final output need to be stored under
  * HDFS location-avro format /user/cloudera/question97/daily_revenue_avro_scala
  * HDFS location-text format /user/cloudera/question97/daily_revenue_txt_scala
  * Local location /home/cloudera/question97
  * Solution need to be stored under /home/cloudera/question97
  */
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table orders \
  --delete-target-dir \
  --target-dir /user/cloudera/question97/orders \
  --outdir /home/cloudera/outdir \
--outdir /home/cloudera/bindir \
--num-mappers 1

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
--num-mappers 1

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
--num-mappers 1

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
result.repartition(1).write.avro("/user/cloudera/question97/daily_revenue_avro_scala")
// HDFS location-text format /user/cloudera/question97/daily_revenue_txt_scala
result.rdd.map(r => r.mkString(",")).repartition(1).saveAsTextFile("/user/cloudera/question97/daily_revenue_txt_scala")

$ hdfs dfs -ls /user/cloudera/question97/daily_revenue_avro_scala
$ hdfs dfs -text /user/cloudera/question97/daily_revenue_avro_scala/part-r-00000-bd34e6ad-39f9-44e0-b677-162031ccfc41.avro
$ hdfs dfs -cat /user/cloudera/question97/daily_revenue_txt_scala/part* | head -n 50