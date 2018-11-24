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
 * 1. Copy "retail_db.orders" and "retail_db.order_items" table to hdfs in respective directory p92_orders and p92_order_items .
 * 2. Join these data using order_id in Spark and Python
 * 3. Calculate total revenue perday and per customer
 * 4. Calculate maximum revenue customer
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1 : Import Single table . 
$ sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \ 
--username retail_dba \
--password=cloudera \
--table=orders \ 
--target-dir=p92_orders \
--num-mappers 1 

$ sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username=retail_dba \
--password=cloudera \
--table order_items \
--target-dir p92_order_orderitems \ 
--num-mappers 1 

//Step 2 : Read the data from one of the partition, created using above command, 
$ hadoop fs -cat p92_orders/part-m-00000 
$ hadoop fs -cat p92 orderitems/part-m-00000 

//Step 3 : Load these above two directory as RDD using Spark and Python (Open pyspark terminal and do following). 
orders = sc.textFile("/files/orders/p*") 
orderitems = sc.textFile("/files/order_items/p*") 

//Step 4 : Convert RDD into key value as (orderjd as a key and rest of the values as a value) 
//First value is orderid 
ordersKeyValue = orders.map(lambda line: (int(line.split(",")[0]), line)) 
//Second value as an Orderid 
orderItemsKeyValue = orderitems.map(lambda line: (int(line.split(",")[1]), line)) 

//Step 5 : Join both the RDD using order_id 
joinedData = orderItemsKeyValue.join(ordersKeyValue) 
//print the joined data 
for line in joinedData.collect(): print(line) 
//Format of joinedData as below. 
//[Orderld, 'All columns from orderltemsKeyValue', 'All columns from ordersKeyValue'] 
ordersPerDatePerCustomer = joinedData.map(lambda line: ((line[1][1].split(",")[1], line[1][1].split(",")[2]), float(line[1][0].split(",")[4]))) 
amountCollectedPerDayPerCustomer = ordersPerDatePerCustomer.reduceByKey(lambda runningSum, amount: runningSum + amount) 
//(Out record format will be ((date,customer_id), totalAmount) 
for line in amountCollectedPerDayPerCustomer.collect(): print(line) 
//now change the format of record as (date,(customer_id,total_amount)) 
revenuePerDatePerCustomerRDD = amountCollectedPerDayPerCustomer.map(lambda threeElementTuple: (threeElementTuple[0][0], (threeElementTuple[0][1],threeElementTuple[1]))) 
for line in revenuePerDatePerCustomerRDD.collect(): print(line) 
//Calculate maximum amount collected by a customer for each day 
perDateMaxAmountCollectedByCustomer = revenuePerDatePerCustomerRDD.reduceByKey(lambda runningAmountTuple, newAmountTuple: (runningAmountTuple if runningAmountTuple[1] >= newAmountTuple[1] else newAmountTuple)) 
for line in perDateMaxAmountCollectedByCustomer.sortByKey().collect(): print(line)

/********SOLUTION WITH SPARK SQL**********/
//Step 1: Copy the tables with sqoop
$ sqoop import \
--connect jdbc:mysql://localhost/retail_db \
--username training \
--password training \
--table orders \
--delete-target-dir \
--target-dir /files/orders \
--outdir /home/training/Desktop/outdir \
--bindir /home/training/Desktop/bindir \
--num-mappers 1

$ sqoop import \
--connect jdbc:mysql://localhost/retail_db \
--username training \
--password training \
--table order_items \
--delete-target-dir \
--target-dir /files/order_items \
--outdir /home/training/Desktop/outdir \
--bindir /home/training/Desktop/bindir \
--num-mappers 1

//Step 2: Check the import
$ hdfs dfs -ls /files/orders
$ hdfs dfs -cat /files/orders/p*
$ hdfs dfs -ls /files/order_items
$ hdfs dfs -cat /files/order_items/p*

//Step 3: Join these data using order_id in Spark and Python
from pyspark.sql.types import *
fields = [StructField("id", IntegerType(), True),StructField("date", StringType(), True),StructField("customer", IntegerType(), True),StructField("status", StringType(), True)]
schema = StructType(fields)
orders = sc.textFile("/files/orders/p*").map(lambda lines: lines.split(",")).map(lambda arr: (int(arr[0]),arr[1],int(arr[2]),arr[3])).toDF(schema)


fields = [StructField("id", IntegerType(), True),StructField("order_id", IntegerType(), True),StructField("product_id", IntegerType(), True),StructField("quantity", IntegerType(), True),StructField("subtotal", FloatType(), True),StructField("price", FloatType(), True)]
schema = StructType(fields)
orderItems = sc.textFile("/files/order_items/p*").map(lambda lines: lines.split(",")).map(lambda arr: (int(arr[0]),int(arr[1]),int(arr[2]),int(arr[3]),float(arr[4]),float(arr[5]))).toDF(schema)

orders.registerTempTable("o")

orderItems.registerTempTable("oi")

joined = sqlContext.sql("SELECT o.id as idOrder, o.date, o.customer, o.status, oi.id as idItem,oi.product_id, oi.quantity, oi.subtotal, oi.price FROM o JOIN oi ON(o.id = oi.order_id)")
joined.show()
joined.registerTempTable("joined")

//Step 4: Calculate total revenue perday and per customer
sqlContext.sql("SELECT sum(subtotal) as `Total Revenue`, date, customer FROM joined GROUP BY date, customer").show()

//4. Calculate maximum revenue customer
sqlContext.sql("SELECT sum(subtotal) as `Total Revenue`, customer FROM joined GROUP BY customer").show()