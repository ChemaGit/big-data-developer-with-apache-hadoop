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

//1. create a hive meta store database named problem6 and import all tables from mysql retail_db database into hive meta store. 

$ beeline -n training -p training -u jdbc:hive2://localhost:10000/default
hive> create database problem6;

sqoop import-all-tables \
--connect jdbc:mysql://localhost/retail_db \
--username training \
--password training \
--warehouse-dir /user/hive/warehouse/problem6.db \
--hive-import \
--hive-database problem6 \
--create-hive-table \
--as-textfile \
--outdir /home/training/Desktop/outdir \
--bindir /home/training/Desktop/outdir \
--autoreset-to-one-mapper

//2. On spark shell use data available on meta store as source and perform step 3,4,5 and 6. [this proves your ability to use meta store as a source]  
var hc = new org.apache.spark.sql.hive.HiveContext(sc);
hc.sql("use problem6")

//3. Rank products within department by price and order by department ascending and rank descending [this proves you can produce ranked and sorted data on joined data sets]
var hiveResult = hc.sql("select   d.department_id,   p.product_id,   p.product_name,  p.product_price,  rank() over (partition by d.department_id order by p.product_price) as product_price_rank,   dense_rank() over (partition by d.department_id order by p.product_price) as product_dense_price_rank   from products p   inner join categories c on c.category_id = p.product_category_id  inner join departments d on c.category_department_id = d.department_id  order by d.department_id, product_price_rank desc, product_dense_price_rank ");

//4. find top 10 customers with most unique product purchases. 
//if more than one customer has the same number of product purchases then the customer with the lowest customer_id will take precedence [this proves you can produce aggregate statistics on joined datasets]
var hiveResult2 = hc.sql("select c.customer_id, c.customer_fname, count(distinct(oi.order_item_product_id)) unique_products from customers c inner join orders o on o.order_customer_id = c.customer_id  inner join order_items oi on o.order_id = oi.order_item_order_id  group by c.customer_id, c.customer_fname  order by unique_products desc, c.customer_id   limit 10")

//5. On dataset from step 3, apply filter such that only products less than 100 are extracted [this proves you can use subqueries and also filter data]
hiveResult.registerTempTable("product_rank_result_temp");
hc.sql("select * from product_rank_result_temp where product_price < 100").show();

//6. On dataset from step 4, extract details of products purchased by top 10 customers which are priced at less than 100 USD per unit [this proves you can use subqueries and also filter data]
var topCustomers = hc.sql("select c.customer_id, c.customer_fname, count(distinct(oi.order_item_product_id)) unique_products  from customers c   inner join orders o on o.order_customer_id = c.customer_id  inner join order_items oi on o.order_id = oi.order_item_order_id  group by c.customer_id, c.customer_fname  order by unique_products desc, c.customer_id   limit 10  ");

topCustomers.registerTempTable("top_cust");

var topProducts = hc.sql("select distinct p.* from products p inner join order_items oi on oi.order_item_product_id = p.product_id inner join orders o on o.order_id = oi.order_item_order_id inner join top_cust tc on o.order_customer_id = tc.customer_id where p.product_price < 100");

//7. Store the result of 5 and 6 in new meta store tables within hive. [this proves your ability to use metastore as a sink]

hc.sql("select * from product_rank_result_temp where product_price < 100").saveAsTable("product_rank_result")
topProducts.saveAsTable("top_products")


hc.sql("create table problem6.product_rank_result as select * from product_rank_result_temp where product_price < 100");


hc.sql("create table problem 6.top_products as select distinct p.* from products p inner join order_items oi on oi.order_item_product_id = p.product_id inner join orders o on o.order_id = oi.order_item_order_id inner join top_cust tc on o.order_customer_id = tc.customer_id where p.product_price < 100");