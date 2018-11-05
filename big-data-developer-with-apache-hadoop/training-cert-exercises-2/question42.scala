/**
 * Problem Scenario 85 : In Continuation of previous question, please accomplish following activities.
 * 1. Select all the columns from product table with output header as below. productID AS ID code AS Code name AS Description price AS 'Unit Price'
 * 2. Select code and name both separated by '-' and header name should be 'Product Description'.
 * 3. Select all distinct prices.
 * 4. Select distinct price and name combination.
 * 5. Select all price data sorted by both code and productID combination.
 * 6. count number of products.
 * 7. Count number of products for each code.
 */
//Solution with SPARK
//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
$ spark-shell
scala> sqlContext.sql("""show databases""")
scala> sqlContext.sql("""use default""")
//Step 1 : Select all the columns from product table with output header as below. productID AS ID code AS Code name AS Description price AS "Unit Price' 
val results = sqlContext.sql("""SELECT productid AS ID, code AS Code, name AS Description, price AS `Unit Price` FROM products ORDER BY ID""") 
results.show() 
//Step 2 : Select code and name both separated by ' -' and header name should be "Product Description. 
val results = sqlContext.sql("""SELECT CONCAT(code,'-', name) AS `Product Description`, price FROM products""" ) 
results.show() 
//Step 3 : Select all distinct prices. 
val results = sqlContext.sql("""SELECT DISTINCT price AS `Distinct Price` FROM products""") 
results.show() 
//Step 4 : Select distinct price and name combination. 
val results = sqlContext.sql("""SELECT DISTINCT price, name FROM products""" ) 
results. show() 
//Step 5 : Select all price data sorted by both code and productID combination. 
val results = sqlContext.sql("""SELECT price FROM products ORDER BY code, productid""") 
results.show() 
//Step 6 : count number of products. 
val results = sqlContext.sql("""SELECT COUNT(productid) AS `Count` FROM products""") 
results.show() 
//Step 7 : Count number of products for each code. 
val results = sqlContext.sql("""SELECT code, COUNT(productid) as `Product Id` FROM products GROUP BY code""") 
results.show() 
val results = sqlContext.sql("""SELECT code, COUNT(productid) AS count FROM products GROUP BY code ORDER BY count DESC""") 
results.show()
//Solution with HIVE
$ hive
hive> show databases;
hive> use default;
hive> show tables;
hive> select productid as ID, code as Code, name as Description, quantity, price as `Unit Price` from products;
hive> select concat(code,'-' ,name) as `Product Description` from products;
hive> select distinct price from products;
hive> select distinct price, name from products;
hive> select code, productid, price from products order by code, productid desc;
hive> select count(productid) from products;
hive> select productid, count(productid) from products group by productid;