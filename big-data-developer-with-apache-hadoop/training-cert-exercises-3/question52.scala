/**
 * Problem Scenario 86 : In Continuation of previous question, please accomplish following activities.
 * 1. Select Maximum, minimum, average , Standard Deviation for price column, and total quantity.
 * 2. Select minimum and maximum price for each product code.
 * 3. Select Maximum, minimum, average , Standard Deviation for price column, and total quantity for each product code, however make sure Average and Standard deviation will have maximum two decimal values.
 * 4. Select all the product code and average price only where product count is more than or equal to 3.
 * 5. Select maximum, minimum , average price and total quantity of all the products for each code. Also produce the same across all the products. 
 */
//First we export product.csv to mysql, then import table product from mysql to hive
sqoop export \
--connect jdbc:mysql://localhost/test \
--username training \
--password training \
--table product \
--export-dir /files/product/product.csv \
--outdir /home/training/Desktop/outdir \
--bindir /home/training/Desktop/bindir \
--num-mappers 1

sqoop import \
--connect jdbc:mysql://localhost/test \
--username training \
--password training \
--table product \
--hive-import \
--hive-home /user/hive/warehouse \
--hive-table hadoopexam.product \
--outdir /home/training/Desktop/outdir \
--bindir /home/training/Desktop/bindir \
--num-mappers 1

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 

sqlContext.sql("show databases").show()
sqlContext.sql("use hadoopexam")
sqlContext.sql("show tables").show()
sqlContext.sql("desc product").show()

//Step 1 : Select Maximum, minimum, average , Standard Deviation for price column, and total quantity. 
val results = sqlContext.sql("SELECT MAX(price) AS MAX , MIN(price) AS MIN , AVG(price) AS Average, STD(price) AS STD, SUM(quantity) AS total_products FROM product") 
results.show() 

//Step 2 : Select minimum and maximum price for each product code. 
val results = sqlContext.sql("SELECT product_code, MAX(price) AS HighestPrice, MIN(price) AS LowestPrice FROM product GROUP BY product_code") 
results.show() 

//Step 3 : Select Maximum, minimum, average , Standard Deviation for price column, and total quantity for each product code, however make sure Average and Standard deviation will have maximum two decimal values. 
val results = sqlContext.sql("SELECT product_code, MAX(price), MIN(price), CAST(AVG(price) AS DECIMAL(7,2)) AS Average, CAST(STD(price) AS DECIMAL(7,2)) AS StdDev, SUM(quantity) FROM product GROUP BY product_code") 
results.show() 

//Step 4 : Select all the product code and average price only where product count is more than or equal to 3. 
val results = sqlContext.sql("SELECT product_code , COUNT(product_code) AS Count, CAST(AVG(price) AS DECIMAL(7,2)) AS Average FROM product GROUP BY product_code HAVING Count >= 3") 
results. show() 

//Step 5 : Select maximum, minimum , average price and total quantity of all the products for each code. Also produce the same across all the products. 
val results = sqlContext.sql( """SELECT product_code, MAX(price), MIN(price), CAST(AVG(price) AS DECIMAL(7,2)) AS Average, SUM(quantity) as total FROM product GROUP BY product_code WITH ROLLUP""" ) 
results.show()

//Other solution
//1. Select Maximum, minimum, average , Standard Deviation, and total quantity.
sqlContext.sql("select max(price) as maxQuantity, min(price) as minQuantity, avg(price) as avgQuantity, stddev_pop(price) as standDevQuantity, sum(quantity) as totalQuantity from product").show()
//2. Select minimum and maximum price for each product code.
sqlContext.sql("select max(price) as maxPrice, min(price) as minPrice, product_code from product group by product_code").show()
//3. Select Maximum, minimum, average , Standard Deviation for price column, and total quantity for each product code, however make sure Average and Standard deviation will have maximum two decimal values.
sqlContext.sql("select product_code, max(price) as maxQuantity, min(price) as minQuantity, format_number(avg(price),2) as avgQuantity, format_number(stddev_pop(price),2) as standDevQuantity, sum(quantity) as totalQuantity from product group by product_code").show()
//4. Select all the product code and average price only where product count is more than or equal to 3.
sqlContext.sql("select product_code,count(product_code) as count, format_number(avg(price),2) as avgPrice from product group by product_code HAVING COUNT(product_code) >= 3").show()
//5. Select maximum, minimum , average price and total quantity of all the products for each code. Also produce the same across all the products. 
sqlContext.sql("select product_code,max(price) as maxPrice, min(price) as minPrice,format_number(avg(price),2) as avgPrice, sum(quantity) as total from product group by product_code with rollup").show()