/**
 * Problem Scenario 4: You have been given MySQL DB with following details.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * table=retail_db.categories
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Please accomplish following activities.
 * Import Single table categories (Subset data) to hive managed table , where category_id between 1 and 22
 */
//Step 1 : Import Single table (Subset data)
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table categories \
--where "category_id between 1 and 22" \
--hive-import \
--hive-table categories \
--hive-overwrite \
--num-mappers 1

Step 2 : Check whether table is created or not (In Hive) 
$ hdfs dfs -ls /user/hive/warehouse/categories
$ hdfs dfs -cat /user/hive/warehouse/categories/part-m-00000

$ hive
hive> show databases;
hive> use retail_db;
hive> show tables;
hive> select * from categories;
hive> quit;