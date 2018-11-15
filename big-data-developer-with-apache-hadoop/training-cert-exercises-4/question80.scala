/** Question 80
 * Problem Scenario 6 : You have been given following mysql database details as well as other info.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Compression Codec : org.apache.hadoop.io.compress.SnappyCodec
 * Please accomplish following.
 * 1. Import entire database such that it can be used as a hive tables, it must be created in default schema.
 * 2. Also make sure each tables file is partitioned in 3 files e.g. part-00000, part-00002, part-00003
 * 3. Store all the Java files in a directory called java_output to evalute the further
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1 : Drop all the tables, which we have created in previous problems. Before implementing the solution. Login to hive and execute following command. 
$ beeline -u cloudera -p cloudera -u jdbc:hive2://localhost:10000/default
hive> show tables; 
hive> drop table categories; 
hive> drop table customers; 
hive> drop table departments; 
hive> drop table employee; 
hive> drop table ordeMtems; 
hive> drop table orders; 
hive> drop table products; 
hive> show tables; 
//Check warehouse directory. 
$ hdfs dfs -ls /user/hive/warehouse 

//Step 2 : Now we have cleaned database. Import entire retail db with all the required parameters as problem statement is asking. 
$ sqoop import-all-tables \ 
--m 3 \ 
--connect jdbc:mysql://quickstart:3306/retail_db \ 
--username retail_dba \ 
--password cloudera \ 
--hive-import \ 
--hive-overwrite \ 
--create-hive-table \ 
--compress \ 
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \ 
--outdir java_output 

//Step 3 : Verify the work is accomplished or not. 
//a. Go to hive and check all the tables hive 
hive> show tables; 
hive> select count(1) from customers; 
//b. Check the-warehouse directory and number of partitions, 
$ hdfs dfs -ls /user/hive/warehouse 
$ hdfs dfs -ls /user/hive/warehouse/categories 
//c. Check the output Java directory. 
$ ls -l java_output/