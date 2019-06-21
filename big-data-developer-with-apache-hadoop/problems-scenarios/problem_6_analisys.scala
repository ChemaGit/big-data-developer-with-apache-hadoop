/**
Problem 6: Provide two solutions for steps 2 to 7
Using HIVE QL over Hive Context
Using Spark SQL over Spark SQL Context or by using RDDs
1. create a hive meta store database named problem6 and import all tables from mysql retail_db database into hive meta store. 
2. On spark shell use data available on meta store as source and perform step 3,4,5 and 6. [this proves your ability to use meta store as a source]  
3. Rank products within department by price and order by department ascending and rank descending [this proves you can produce ranked and sorted data on joined data sets]
4. find top 10 customers with most unique product purchases. if more than one customer has the same number of product purchases then the customer with the lowest customer_id will take precedence [this proves you can produce aggregate statistics on joined datasets]
5. On dataset from step 3, apply filter such that only products less than 100 are extracted [this proves you can use subqueries and also filter data]
6. On dataset from step 4, extract details of products purchased by top 10 customers which are priced at less than 100 USD per unit [this proves you can use subqueries and also filter data]
7. Store the result of 5 and 6 in new meta store tables within hive. [this proves your ability to use metastore as a sink]
  */
// 1. create a hive meta store database named problem6 and import all tables from mysql retail_db database into hive meta store.
$ hive
  hive> create database problem6;

sqoop import-all-tables \
  --connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --hive-import \
--hive-database problem6 \
--create-hive-table \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--autoreset-to-one-mapper

hive> use problem6;
hive> show tables;
hive> select * from orders limit 10;

// 2. On spark shell use data available on meta store as source and perform step 3,4,5 and 6. [this proves your ability to use meta store as a source]
sqlContext.sql("use problem6")
// 3. Rank products within department by price and order by department ascending and rank descending [this proves you can produce ranked and sorted data on joined data sets]
var hiveResult = sqlContext.sql("""SELECT d.department_id,p.product_id,p.product_name,p.product_price,rank() over(partition by d.department_id order by p.product_price) as product_price_rank,dense_rank() over(partition by d.department_id order by p.product_price) as product_dense_price_rank FROM products p INNER JOIN categories c ON(c.category_id = p.product_category_id) INNER JOIN departments d ON(c.category_department_id = d.department_id) ORDER BY d.department_id,product_price_rank desc,product_dense_price_rank """);

// 4. find top 10 customers with most unique product purchases. if more than one customer has the same number of product purchases then the customer with the lowest customer_id will take precedence [this proves you can produce aggregate statistics on joined datasets]
val hiveResult2 = sqlContext.sql("""SELECT c.customer_id,c.customer_fname,COUNT(DISTINCT(oi.order_item_product_id)) AS unique_products FROM customers c INNER JOIN orders o ON(c.customer_id = o.order_customer_id) INNER JOIN order_items oi ON(o.order_id = oi.order_item_order_id) GROUP BY c.customer_id,c.customer_fname ORDER BY unique_products DESC, c.customer_id LIMIT 10""")

// 5. On dataset from step 3, apply filter such that only products less than 100 are extracted [this proves you can use subqueries and also filter data]
hiveResult.registerTempTable("dataset3")
val hiveResult3 = sqlContext.sql("""SELECT * FROM dataset3 WHERE product_price < 100""")

// 6. On dataset from step 4, extract details of products purchased by top 10 customers which are priced at less than 100 USD per unit [this proves you can use subqueries and also filter data]
hiveResult2.registerTempTable("dataset4")
val hiveResult4 = sqlContext.sql("""SELECT DISTINCT p.* FROM products p JOIN order_items oi ON(oi.order_item_product_id = p.product_id) JOIN orders o ON(o.order_id = oi.order_item_order_id) JOIN dataset4 dt4 ON(o.order_customer_id = dt4.customer_id) WHERE p.product_price < 100""")