/** Question 90
 * Problem Scenario 75 : You have been given MySQL DB with following details.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * table=retail_db.orders
 * table=retail_db.order_items
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Please accomplish following activities.
 * 1. Copy "retail_db.order_items" table to hdfs in respective directory p90_order_items .
 * 2. Do the summation of entire revenue in this table using pyspark.
 * 3. Find the maximum and minimum revenue as well.
 * 4. Calculate average revenue
 * Columns of orde_items table : (order_item_id , order_item_order_id ,order_item_product_id, order_item_quantity,order_item_subtotal,order_item_product_price)
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 

//Step 1 : Import Single table . 
$ sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \ 
--username retail_dba \
--password cloudera \ 
--table order_items \ 
--target-dir p90_order_items \
--num-mappers 1  

//Step 2 : Read the data from one of the partition, created using above command. 
$ hadoop fs -cat p90_order_items/part-m-00000 

//Step 3 : In pyspark, get the total revenue across all days and orders. 
$ pyspark
> entireTableRDD = sc.textFile("p90_order_items") 
#Cast string to float 
> extractedRevenueColumn = entireTableRDD.map(lambda line: float(line.split(",")[4])) 

//Step 4 : Verify extracted data 
> for revenue in extractedRevenueColumn.collect(): print revenue 

#use reduce'function to sum a single column value 
> totalRevenue = extractedRevenueColumn.reduce(lambda a, b: a + b) 

//Step 5 : Calculate the maximum revenue 
> maximumRevenue = extractedRevenueColumn.reduce(lambda a, b: (a if a>=b else b)) 

//Step 6 : Calculate the minimum revenue 
> minimumRevenue = extractedRevenueColumn.reduce(lambda a, b: (a if a<=b else b)) 

//Step 7 : Caclculate average revenue 
> count=extractedRevenueColumn.count() 
> averageRev=totalRevenue/count

/******SOLUTION IN SPARK SQL**********/

//Check database reail_db
$ mysql -u training -p training
mysql> use retail_db;
mysql> show tables;
mysql> source order_items.sql

//Step 1: Copy "retail_db.order_items" table to hdfs in respective directory p90_order_items . 
$ sqoop import \
--connect jdbc:mysql://localhost/retail_db \
--username training \
--password training \
--table order_items \
--target-dir /files/p90_order_items \
--delete-target-dir \
--outdir /home/training/Desktop/outdir \
--bindir /home/training/Desktop/bindir \
--num-mappers 1

//Check the import in hdfs
$ hdfs dfs -ls /files/p90_order_items
$ hdfs dfs -cat /files/p90_order_items/p*

//Step 2: Do the summation of entire revenue in this table using pyspark.
$ pyspark
> from pyspark.sql.types import *
> fields = [StructField("id", IntegerType(), True), StructField("id_order", IntegerType(), True), StructField("id_product", IntegerType(), True), StructField("quantity", IntegerType(), True), StructField("subtotal", IntegerType(), True), StructField("price", IntegerType(), True)]
> schema = StructType(fields)
> orderItems = sc.textFile("/files/p90_order_items/*").map(lambda lines: lines.split(",")).map(lambda arr: (int(arr[0]), int(arr[1]),int(arr[2]),int(arr[3]),int(arr[4]),int(arr[5])) ).toDF(schema)
> orderItems.registerTempTable("order_items")
> sqlContext.sql("select * from order_items").show()
> sqlContext.sql("select sum(subtotal) as `Sum Revenue` from order_items").show()

//Step 3: Find the maximum and minimum revenue as well.
> sqlContext.sql("select max(subtotal) as `Max Revenue`, min(subtotal) as `Min Revenue` from order_items").show()

//Step 4: Calculate average revenue
> sqlContext.sql("select avg(subtotal) as `Average Revenue` from order_items").show()