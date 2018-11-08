/**
 * Problem Scenario 74 : You have been given MySQL DB with following details.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * table=retail_db.orders
 * table=retail_db.order_items
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Columns of orders table : (orderid , order_date , ordercustomerid, order_status)
 * Columns of order_items table : (order_item_id , order_item_order_id ,order_item_product_id, order_item_quantity,order_item_subtotal,order_item_product_price)
 * Please accomplish following activities.
 * 1. Copy "retaildb.orders" and "retaildb.order_items" table to hdfs in respective directory p89_orders and p89_order_items . 
 * 2. Join these data using orderid in Spark and Python 
 * 3. Now fetch selected columns from joined data Orderid, Order_date and amount collected on this order.
 * 4. Calculate total order placed for each date, and produced the output sorted by date.
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution: 
//Step 1 : Import Single table . 
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table orders \
--target-dir p89_orders \
--num-mappers 1 

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table order_items \
--target-dir p89_order_items \
--num-mappers 1 

//Step 2 : Read the data from one of the partition, created using above command, 
$ hadoop fs -cat p89_orders/part-m-00000 
$ hadoop fs -cat p89_order_items/part-m-00000 

//Step 3 : Load these above two directory as RDD using Spark and Python (Open pyspark terminal and do following). 
orders = sc.textFile("p89_orders") 
orderItems = sc.textFile("p89_order_items") 

//Step 4 : Convert RDD into key value as (orderid as a key and rest of the values as a value) 
//First value is orderid 
ordersKeyValue = orders.map(lambda line: (int(line.split(",")[0]), line)) 
//Second value as an Orderid 
orderItemsKeyValue = orderItems.map(lambda line: (int(line.split(",")[1]), line)) 

//Step 5 : Join both the RDD using orderid 
joinedData = orderItemsKeyValue.join(ordersKeyValue) 
//print the joined data 
for line in joinedData.collect(): print(line) 
//Format of joinedData as below. [Orderid, 'All columns from orderItemsKeyValue', 'All columns from orders Key Value'] 

//Step 6 : Now fetch selected values Orderid, Order date and amount collected on this order. 
revenuePerOrderPerDay = joinedData.map(lambda row: (row[0], row[1][1].split(",")[1], float(row[1][0].split(",")[4]))) 
//print the result 
for line in revenuePerOrderPerDay.collect(): print(line) 
//Step 7 : Select distinct order ids for each date. 
//distinct(date,order_id) 
distinctOrdersDate = joinedData.map(lambda row: row[1][1].split(",")[1] + "," + str(row[0]) ).distinct()
for line in distinctOrdersDate.collect(): print(line) 

//Step 8 : Similar to word count, generate (date, 1) record for each row. 
newLineTuple = distinctOrdersDate.map(lambda line: (line.split(",")[0], 1)) 

//Step 9 : Do the count for each key(date), to get total order per date. 
totalOrdersPerDate = newLineTuple.reduceByKey(lambda a, b: a + b) 
//print results 
for line in totalOrdersPerDate.collect(): print(line) 

//step 10 : Sort the results by date 
sortedData = totalOrdersPerDate.sortByKey().collect() 
//print results 
for line in sortedData: print(line)

//Solution in SparkSQL
sc.textFile("p89_orders").map(lambda line: line.split(",")).map(lambda arr: (int(arr[0]),arr[1],int(arr[2]),arr[3])).toDF(["id","date","customer","status"]).registerTempTable("orders")
sc.textFile("p89_order_items").map(lambda line: line.split(",")) \
.map(lambda arr: (int(arr[0]),int(arr[1]),int(arr[2]),int(arr[3]),int(arr[4]),int(arr[5]))) \
.toDF(["id","idorder","idproduct","quantity","subtotal","price"]).registerTempTable("order_item")
sqlContext.sql("select * from orders").show()
sqlContext.sql("select * from order_item").show()
joined = sqlContext.sql("select orders.id, orders.date, order_item.subtotal from orders join order_item on orders.id = order_item.idorder")
joined.show()
joined.registerTempTable("joined")
distinctDate = sqlContext.sql("select id, date from joined group by id, date")
distinctDate.show()
distinctDate.registerTempTable("distinctDate")
distinctCount = sqlContext.sql("select date, count(id) as `count` from distinctDate group by date order by date")
distinctCount.show()