/**
 * Problem Scenario 76 : You have been given MySQL DB with following details.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * table=retail_db.orders
 * table=retail_db.order_items
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Columns of order table : (orderid , order_date , ordercustomerid, order_status)
 * Columns of order_items table : (order_item_id , order_item_order_id ,order_item_product_id, order_item_quantity,order_item_subtotal,order_item_product_price)
 * Please accomplish following activities.
 * 1. Copy "retail_db.orders" table to hdfs in a directory p91_orders.
 * 2. Once data is copied to hdfs, using pyspark calculate the number of order for each status.
 * 3. Use all the following methods to calculate the number of order for each status. (You need to know all these functions and its behavior for real exam)
 * -countByKey()
 * -groupByKey()
 * -reduceByKey()
 * -aggregateByKey()
 * -combineByKey()
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1 : Import Single table 
$ sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table orders \
--delete-target-dir \
--target-dir p91_orders \
--num-mappers 1

//Step 2 : Read the data from one of the partition, created using above command, 
$ hadoop fs -cat p91_orders/part-m-00000 

//(These are very important functions for real exam)
//Step 3: countByKey #Number of orders by status 
allOrders = sc.textFile("p91_orders") 
//#Generate key and value pairs (key is order status and value as an empty string 
keyValue = allOrders.map(lambda line: (line.split(",")[3], "")) 
//#Using countByKey, aggregate data based on status as a key 
output = keyValue.countByKey().items()
for line in output: print(line) 

//Step 4 : groupByKey #Generate key and value pairs (key is order status and value as an one 
keyValue = allOrders.map(lambda line: (line.split(",")[3], 1)) 
//#Using groupByKey, aggregate data based on status as a key 
output = keyValue.groupByKey().map(lambda kv: (kv[0], sum(kv[1]))) 
for line in output.collect(): print(line) 

//Step 5 : reduceByKey #Generate key and value pairs (key is order status and value as an one 
keyValue = allOrders.map(lambda line: (line.split(",")[3], 1)) 
//#Using reduceByKey, aggregate data based on status as a key 
output = keyValue.reduceByKey(lambda a, b: a + b) 
for line in output.collect(): print(line) 

//Step 6: aggregateByKey #Generate key and value pairs (key is order status and value as an one 
keyValue = allOrders.map(lambda line: (line.split(",")[3], line))
output = keyValue.aggregateByKey(0, lambda a, b: a + 1, lambda a, b: a + b) 
for line in output.collect(): print(line) 

//Step 7 : combineByKey #Generate key and value pairs (key is order status and vale as an one 
keyValue = allOrders.map(lambda line: (line.split(",")[3], line)) 
output = keyValue.combineByKey(lambda value: 1, lambda acc, value: acc + 1, lambda acc, value: acc + value) 
for line in output.collect(): print(line) 

//Solution in Scala
//Step 3: countByKey #Number of orders by status
val allOrders = sc.textFile("p91_orders")
val keyValue = allOrders.map(line => (line.split(",")(3), ""))
val output = keyValue.countByKey().toList
output.foreach(println)

//Step 4 : groupByKey #Generate key and value pairs (key is order status and value as an one 
val keyValue = allOrders.map(line => (line.split(",")(3), 1))
val output = keyValue.groupByKey().map(tuple => (tuple._1, tuple._2.size))
output.collect().foreach(println)

//Step 5 : reduceByKey #Generate key and value pairs (key is order status and value as an one
val keyValue = allOrders.map(line => (line.split(",")(3), 1))
val output = keyValue.reduceByKey({case (v, v1) => v + v1})
output.collect().foreach(println)

//Step 6: aggregateByKey #Generate key and value pairs (key is order status and value as an one 
val keyValue = allOrders.map(line => (line.split(",")(3), line))
val output = keyValue.aggregateByKey(0)((init,v) => init + 1, (v, v1) => v + v1)
output.collect().foreach(println)

//Step 7 : combineByKey #Generate key and value pairs (key is order status and vale as an one 
val keyValue = allOrders.map(line => (line.split(",")(3), line))
val output = keyValue.combineByKey(init => 1, (acc: Int, value) => acc + 1, (init: Int, acc: Int) => init + acce)
output.collect().foreach(println)