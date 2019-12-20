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
  */
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
/*
+---+-----------+
|cId|max_revenue|
+---+-----------+
|791|   10524.17|
+---+-----------+
*/
