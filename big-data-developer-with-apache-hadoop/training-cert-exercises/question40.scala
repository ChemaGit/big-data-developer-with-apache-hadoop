/**
 * Problem Scenario 8 : You have been given following mysql database details as well as other info.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Please accomplish following.
 * 1. Import joined result of orders and order_items table join on orders.order_id = order_items.order_item_order_id.
 * 2. Also make sure each tables file is partitioned in 2 files e.g. part-00000, part-00002
 * 3. Also make sure you use order_id columns for sqoop to use for boundary conditions.
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solutions: 
//Step 1 : Clean the hdfs file system, if they exists clean out. 
hadoop fs -rm -r departments 
hadoop fs -rm -r categories 
hadoop fs -rm -r products 
hadoop fs -rm -r orders 
hadoop fs -rm -r order_items 
hadoop fs -rm -r customers 
//Step 2 : Now import the order_join table as per requirement. 
sqoop import \ 
--connect jdbc:mysql://quickstart:3306/retail_db \ 
--username=retail_dba \ 
--password=cloudera \ 
--query="select * from orders, order_items where orders.orderid = order_items.order_item_order_id and \$CONDITIONS" \ 
--delete-target-dir \
--target-dir /user/cloudera/order_join \ 
--split-by order_id \ 
--num-mappers 2 
//Step 3 : Check imported data. 
hdfs dfs -ls order_join 
hdfs dfs -cat order_join/part-m-00000 
hdfs dfs -cat order_join/part-m-00001