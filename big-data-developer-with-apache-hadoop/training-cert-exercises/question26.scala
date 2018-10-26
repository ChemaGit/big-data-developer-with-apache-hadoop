/**
 * Problem Scenario 16 : You have been given following mysql database details as well as other info.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Please accomplish below assignment.
 * 1. Create a table in hive as below.
 * create table departments_hive(department_id int, department_name string);
 * 2. Now import data from mysql table departments to this hive table. Please make sure that
 * data should be visible using below hive command, select * from departments_hive
 */
//Step 1. Create a table in hive 
$ hive
hive> show databases;
hive> use hadoopexam;
hive> show tables;
hive> create table departments_hive(department_id int, department_name string);
hive> show tables;
//Step 2. Now import data from mysql table departments to this hive table.
//The important here is, when we create a table without delimiter fields. Then default delimiter for hive is ^A (\001). Hence, while importing data we have to provide proper delimiter. 
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table departments \
--hive-home /user/hive/warehouse \
--hive-import \
--hive-table hadoopexam.departments_hive \
--hive-overwrite \
--fields-terminated-by '\001' \
--num-mappers 1

//Step 3: check the results
hdfs dfs -ls /user/hive/warehouse/hadoopexam.db/departments_hive 
hdfs dfs -cat/user/hive/warehouse/hadoopexam.db/departments_hive/part-m-00000

hive> select * from departments_hive;
hive> quit;