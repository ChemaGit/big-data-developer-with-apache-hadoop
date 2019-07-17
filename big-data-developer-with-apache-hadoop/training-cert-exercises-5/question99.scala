/** Question 99
  *   	- Task 2: Get revenue for each order_item_order_id
  *       	- Define function getRevenuePerOrder with 1 argument order_items
  *       	- Use map reduce APIs to get order_item_order_id and order_item_subtotal, then group by order_item_order_id and then process the values for each order_item_order_id
  *       	- Return a collection which contain order_item_order_id and revenue_per_order_id
  */

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table order_items \
  --as-textfile \
  --delete-target-dir \
  --target-dir /user/cloudera/question99/order_items \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1


val orderItems = sc.textFile("/user/cloudera/question99/order_items").map(line => line.split(","))

def getOrderRevenue(order_items: org.apache.spark.rdd.RDD[Array[String]]): List[(Int, Double)] = {
  val tuple = order_items.map(arr => (arr(1).toInt, arr(4).toDouble))
  val reduce = tuple.reduceByKey( (v,c) => v + c).sortByKey()
  reduce.collect.toList
}

val res = getOrderRevenue(orderItems)
res.foreach(x => println(x))