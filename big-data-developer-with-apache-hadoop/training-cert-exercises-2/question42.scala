/** Question 42
productID,code,name,quantity,price,idSub
1001,PEN,Pen Red,5000,1.23,501
1002,PEN,Pen Blue,8000,1.25,501
1003,PEN,Pen Black,2000,1.25,501
1004,PEC,Pencil 2B,10000,0.48,502
1005,PEC,Pencil 2H,8000,0.49,502
1006,PEC,Pencil HB,0,9999.99,502
2001,PEC,Pencil 3B,500,0.52,501
2002,PEC,Pencil 4B,200,0.62,501
2003,PEC,Pencil 5B,100,0.73,501
2004,PEC,Pencil 6B,500,0.47,502
  * Problem Scenario 85 : In Continuation of previous question, please accomplish following activities.
  * 1. Select all the columns from product table with output header as below. productID AS ID code AS Code name AS Description price AS 'Unit Price'
  * 2. Select code and name both separated by '-' and header name should be 'Product Description'.
  * 3. Select all distinct prices.
  * 4. Select distinct price and name combination.
  * 5. Select all price data sorted by both code and productID combination.
  * 6. count number of products.
  * 7. Count number of products for each code.
  */

// SPARK SOLUTION
val product = sc.textFile("/user/cloudera/files/product.csv").map(lines => lines.split(",")).filter(r => r(0) != "productID").map(r => (r(0).toInt,r(1),r(2),r(3).toInt,r(4).toFloat,r(5).toInt)).toDF("productID","code","name","quantity","price","idSub")

product.registerTempTable("pr")

// 1. Select all the columns from product table with output header as below. productID AS ID code AS Code name AS Description price AS 'Unit Price'
sqlContext.sql("""SELECT productID as ID,code as Code,name as Description, price as `Unit Price` FROM pr""").show()

// 2. Select code and name both separated by '-' and header name should be 'Product Description'.
sqlContext.sql("""SELECT concat(code,"-",name) AS `Product Description` FROM pr""").show()

// 3. Select all distinct prices.
sqlContext.sql("""SELECT DISTINCT(price) FROM pr""").show()

// 4. Select distinct price and name combination.
sqlContext.sql("""SELECT DISTINCT(name), price FROM pr""").show()

// 5. Select all price data sorted by both code and productID combination.
sqlContext.sql("""SELECT productID, code, price from pr ORDER BY productID, code""").show()

// 6. count number of products.
sqlContext.sql("""SELECT COUNT(productID) as `Total Products` FROM pr""").show()

// 7. Count number of products for each code.
sqlContext.sql("""SELECT code, count(productID) FROM pr GROUP BY code""").show()

//HIVE SOLUTION
$ hive
  hive> use pruebas;
hive> show tables;
hive> SELECT productid as ID,productcode as Code,name as Description, price as `Unit Price` FROM t_product_parquet;
hive> SELECT concat(productcode,"-",name) AS `Product Description` FROM t_product_parquet;
hive> SELECT DISTINCT(price) FROM t_product_parquet;
hive> SELECT DISTINCT(name), price FROM t_product_parquet;
hive> SELECT productid, productcode, price from t_product_parquet ORDER BY productid, productcode;
hive> SELECT COUNT(productid) as `Total Products` FROM t_product_parquet;
hive> SELECT productcode, count(productid) FROM t_product_parquet GROUP BY productcode;