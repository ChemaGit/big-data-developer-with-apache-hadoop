/** Question 98
  *   	- Task 1: Get revenue for given order_item_order_id
  *       	- Define function getOrderRevenue with 2 arguments order_items and order_id
  *       	- Use map reduce APIs to filter order items for given order id, to extract order_item_subtotal and add it to get revenue
  *       	- Return order revenue
  */
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username retail_dba \
  --password cloudera \
  --table order_items \
  --as-textfile \
  --delete-target-dir \
  --target-dir /user/cloudera/question98/order_items \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

val orderItems = sc.textFile("/user/cloudera/question98/order_items").map(line => line.split(","))

def getOrderRevenue(order_items: org.apache.spark.rdd.RDD[Array[String]], order_id: Int): Double = {
  val filtered = order_items.filter(arr => arr(1).toInt == order_id)
  val result = filtered.map(arr => arr(4).toDouble).reduce( (v,c) => v + c)
  result
}

val res = getOrderRevenue(orderItems,2)