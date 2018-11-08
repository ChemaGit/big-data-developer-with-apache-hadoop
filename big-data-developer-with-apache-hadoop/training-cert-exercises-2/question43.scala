/**
 * Problem Scenario 77 : You have been given MySQL DB with following details.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * table=retail_db.orders
 * table=retail_db.order_items
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Columns of order table : (orderid , order_date , order_customer_id, order_status)
 * Columns of ordeMtems table : (order_item_id , order_item_order_ld ,order_item_product_id, order_item_quantity,order_item_subtotal,order_item_product_price)
 * Please accomplish following activities.
 * 1. Copy "retail_db.orders" and "retail_db.order_items" table to hdfs in respective directory p92_orders and p92_order_items .
 * 2. Join these data using orderid in Spark and Python
 * 3. Calculate total revenue perday and per order
 * 4. Calculate total and average revenue for each date. - combineByKey -aggregateByKey
 */

//Explanation: Solution : 
//Step 1 : Import Single table . 
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table orders \
--target-dir p92_orders \
--num-mappers 1 

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table order_items \
--target-dir p92_order_items \
--num-mappers 1

//Step 2 : Read the data from one of the partition, created using above command, 
$ hadoop fs -cat p92_orders/part-m-00000 
$ hadoop fs -cat p92_order_items/part-m-00000 

//Step 3 : Load these above two directory as RDD using Spark and Python (Open pyspark terminal and do following). 
orders = sc.textFile("p92_orders") 
orderItems = sc.textFile("p92_order_items") 

//Step 4 : Convert RDD into key value as (orderjd as a key and rest of the values as a value) #First value is orderjd 
ordersKeyValue = orders.map(lambda line: (int(line.split(",")[0]), line)) 
//Second value as an Orderid 
orderItemsKeyValue = orderItems.map(lambda line: (int(line.split(",")[1]), line)) 

//Step 5 : Join both the RDD using orderid 
joinedData = orderItemsKeyValue.join(ordersKeyValue) 
//print the joined data 
for line in joinedData.collect(): print(line) 
//Format of joinedData as below. 
//[OrderId, 'All columns from orderItemsKeyValue', 'All columns from orders Key Value'] 

//Step 6 : Now fetch selected values OrderId, Order date and amount collected on this order.
//Returned row will contain ((order_date,order_id),amout_collected) 
revenuePerDayPerOrder = joinedData.map(lambda row: ( (row[1][1].split(",")[1],row[0]), float(row[1][0].split(",")[4])) ) 
//print the result 
for line in revenuePerDayPerOrder.collect(): print(line) 

//Step 7 : Now calculate total revenue perday and per order A. Using reduceByKey 
totalRevenuePerDayPerOrder = revenuePerDayPerOrder.reduceByKey(lambda runningSum, value: runningSum + value) 
for line in totalRevenuePerDayPerOrder.sortByKey().collect(): print(line) 
//Generate data as (date, amount_collected) (Ignore orderId) 
dateAndRevenueTuple = totalRevenuePerDayPerOrder.map(lambda line: (line[0][0], line[1])) 
for line in dateAndRevenueTuple.sortByKey().collect(): print(line) 

//Step 8 : Calculate total amount collected for each day. And also calculate number of days. 
//Generate output as (Date, Total Revenue for date, total_number_of_dates) 
//Line 1 : it will generate tuple (revenue, 1) 
//Line 2 : Here, we will do summation for all revenues at the same time another counter to maintain number of records. 
//Line 3 : Final function to merge all the combiner 
totalRevenueAndTotalCount = dateAndRevenueTuple.combineByKey(lambda revenue: (revenue, 1), \
lambda revenueSumTuple, amount: (revenueSumTuple[0] + amount, revenueSumTuple[1] + 1), \
lambda tuple1, tuple2: (round(tuple1[0] + tuple2[0], 2),tuple1[1] + tuple2[1]) )

for line in totalRevenueAndTotalCount.collect(): print(line) 

//Step 9 : Now calculate average revenue for each date
averageRevenuePerDate = totalRevenueAndTotalCount.aggregateByKey(0.0, lambda init, tuple: (init, tuple[0] / tuple[1]), lambda uno,dos: (uno[0] + dos[0], uno[1] + dos[1]))
for line in averageRevenuePerDate.collect(): print(line)

//Solution in SparkSQL Scala
case class Orders(id: Int, date: String,custId: Int,status: String)
case class OrderItem(itemId: Int,orderId: Int,productId: Int,quantity: Int,subtotal: Int,price: Double)
val orders = sc.textFile("/loudacre/orders/").map(line => line.split(",")).map(arr => new Orders(arr(0).toInt, arr(1), arr(2).toInt, arr(3))).toDF()
orders.registerTempTable("ORDERS")
sqlContext.sql("select * from ORDERS").show()
val orderItems = sc.textFile("/loudacre/order_items/").map(line => line.split(",")).map(arr => new OrderItem(arr(0).toInt,arr(1).toInt,arr(2).toInt,arr(3).toInt,arr(4).toInt,arr(5).toDouble)).toDF()
orderItems.registerTempTable("ORDER_ITEM")
sqlContext.sql("select * from ORDER_ITEM").show()

//Join the data
val results = sqlContext.sql("select ORDERS.date, ORDERS.id, ORDER_ITEM.price FROM ORDERS join ORDER_ITEM on ORDERS.id = ORDER_ITEM.itemId")
results.show()
results.registerTempTable("joined")

//Step 7 : Now calculate total revenue perday and per order
val results = sqlContext.sql("select date, sum(price) as `revenue` from joined group by date, id")
results.show()
results.registerTempTable("sumRevenue")
//Step 8 : Calculate total amount collected for each day. And also calculate number of days.
val results = sqlContext.sql("select sum(revenue) as `sumPrice`, count(date) as `countDays` from sumRevenue group by date")
results.show()
results.registerTempTable("totalRevenue")
////Step 9 : Now calculate average revenue for each date
val results = sqlContext.sql("select sumPrice / countDays as `average` from totalRevenue")
results.show()