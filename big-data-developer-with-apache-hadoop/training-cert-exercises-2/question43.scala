/** Question 43
  * Problem Scenario 77 : You have been given MySQL DB with following details.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * table=retail_db.orders
  * table=retail_db.order_items
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Columns of orders table : (orderid , order_date , order_customer_id, order_status)
  * Columns of order_items table : (order_item_id , order_item_order_ld ,order_item_product_id, order_item_quantity,order_item_subtotal,order_item_product_price)
  * Please accomplish following activities.
  * 1. Copy "retail_db.orders" and "retail_db.order_items" table to hdfs in respective directory question43/orders and question43/order_items .
  * 2. Join these data using orderid in Spark and Scala
  * 3. Calculate total revenue perday and per order
  * 4. Calculate total and average revenue for each date. - combineByKey -aggregateByKey
  */
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table orders \
  --as-textfile \
  --delete-target-dir \
  --target-dir /user/cloudera/question43/orders \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table order_items \
  --as-textfile \
  --delete-target-dir \
  --target-dir /user/cloudera/question43/order_items \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

val orders = sc.textFile("/user/cloudera/question43/orders").map(line => line.split(",")).map(r => (r(0).toInt,r(1).substring(0,10)))
val orderItems = sc.textFile("/user/cloudera/question43/order_items").map(line => line.split(",")).map(r => (r(1).toInt,r(4).toFloat))
val joined = orders.join(orderItems)  // joined: org.apache.spark.rdd.RDD[(Int, (String, Float))]

// 3. Calculate total revenue perday and per order
val selectData = joined.map({case( (id,(date,subtotal)) ) => ((id,date),subtotal)})
val result = selectData.reduceByKey( (v,c) => v + c)
result.collect.foreach(println)

// 4. Calculate total and average revenue for each date. - combineByKey -aggregateByKey
val selectData = joined.map({case( (id,(date,subtotal)) ) => (date,(subtotal,1))})
// reduceByKey
val reduceByKey = selectData.reduceByKey( (v,c) => (v._1 + c._1, v._2 + c._2) ).mapValues({case(s,n) => (s, s / n)}).sortByKey()
reduceByKey.take(10).foreach(println)
// combineByKey
val combineByKey = selectData.combineByKey( ( (v: (Float, Int)) => (v._1, v._2)) , (  ( c: (Float, Int),v: (Float, Int) ) => (c._1 + v._1, c._2 + v._2) ) , ( (v: (Float, Int),c: (Float, Int)) => (v._1 + c._1, v._2 + c._2) ) )
combineByKey.mapValues({case(s,n) => (s, s / n)}).sortByKey().take(10).foreach(println)
// aggregateByKey
val aggregateByKey = selectData.aggregateByKey( (0.0F,0))( ( (z:(Float,Int), v:(Float, Int)) => (z._1 + v._1, z._2 + v._2)),( (v: (Float, Int),c: (Float, Int)) => (v._1 + c._1, v._2 + c._2))).mapValues({case(s,n) => (s, s / n)})
aggregateByKey.sortByKey().take(10).foreach(println)

// SPARK-SQL SOLUTION
val orders = sc.textFile("/user/cloudera/question43/orders").map(line => line.split(",")).map(r => (r(0).toInt,r(1).substring(0,10))).toDF("id_order","date")
val orderItems = sc.textFile("/user/cloudera/question43/order_items").map(line => line.split(",")).map(r => (r(1).toInt,r(4).toFloat)).toDF("item_id_order","subtotal")

orders.registerTempTable("orders")
orderItems.registerTempTable("order_item")
// 3. Calculate total revenue perday and per order
val result = sqlContext.sql("""select date, id_order, round(sum(subtotal),2) as total_revenue from orders join order_item on(id_order = item_id_order) group by date,id_order order by date""")
result.show()
// // 4. Calculate total and average revenue for each date.
val result2 = sqlContext.sql("""select date, round(sum(subtotal),2) as total_revenue,round(avg(subtotal),2) as avg_revenue from orders join order_item on(id_order = item_id_order) group by date order by date""")
result2.show(10)