/**
 * Problem Scenario 1:
 * You have been given MySQL DB with following details.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * table=retail_db.categories
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Please accomplish following activities.
 * 1. Connect MySQL DB and check the content of the tables.
 * 2. Copy "retaildb.categories" table to hdfs, without specifying directory name.
 * 3. Copy "retaildb.categories" table to hdfs, in a directory name "categories_target".
 * 4. Copy "retaildb.categories" table to hdfs, in a warehouse directory name "categories_warehouse".
 */
$mysql -u retail_db -p
> use retail_db;
> show tables;
> select count(*) from categories;
> select * from categories LIMIT 5;
> exit;

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_db --password cloudera \
--table categories 

$hdfs dfs -ls categories
$hdfs dfs -cat categories/part-m-00000 | head -n 20

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_db --password cloudera \
--table categories \
--delete-target-dir \
--target-dir categories_target 

$hdfs dfs -ls categories_target
$hdfs dfs -cat categories_target/part-m-00000 | head -n 20

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_db --password cloudera \
--table categories \
--warehouse-dir categories_warehouse

$hdfs dfs -ls categories_warehouse
$hdfs dfs -cat categories_warehouse/part-m-00000 | head -n 20