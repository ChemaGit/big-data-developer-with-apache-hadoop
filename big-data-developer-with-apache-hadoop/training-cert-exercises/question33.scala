/**
 * Problem Scenario 17 : You have been given following mysql database details as well as other info.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Please accomplish below assignment.
 * 1. Create a table in hive as below, create table departments_hiveO1(department_id int,department_name string, avg_salary int);
 * 2. Create another table in mysql using below statement CREATE TABLE IF NOT EXISTS departments_hive01(id int, department_name varchar(45), avg_salary int);
 * 3. Copy all the data from departments table to departments_hive01 using insert into departments_hive01 select a.*, null from departments a;
 * Also insert following records as below
 * insert into departments_hive01 values(777, "Not known",1000);
 * insert into departments_hive01 values(8888, null,1000);
 * insert into departments_hive01 values(666, null,1100);
 * 4. Now import data from mysql table departments_hive01 to this hive table. Please make
 * sure that data should be visible using below hive command. Also, while importing if null
 * value found for department_name column replace it with "" (empty string) and for avg column with -999 
 * select * from departments_hive01;
 */
//Step 1. Create a table in hive as below, create table departments_hiveO1(department_id int,department_name string, avg_salary int);
$ hive
hive> show databases;
hive> use hadoopexam;
hive> create table departments_hiveO1(department_id int,department_name string, avg_salary int);
hive> show tables;
hive> quit;
//Step 2. Create another table in mysql using below statement CREATE TABLE IF NOT EXISTS departments_hive01(id int, department_name varchar(45), avg_salary int);
$ mysql -u retail_dba -p cloudera
mysql> show databases;
mysql> use retail_db;
mysql> CREATE TABLE IF NOT EXISTS departments_hive01(id int, department_name varchar(45), avg_salary int);
mysql> show tables;
//Step 3. Copy all the data from departments table to departments_hive01 using insert into departments_hive01 select a.*, null from departments a;
mysql> insert into departments_hive01 select a.*, null from departments a;
//Also insert following records as below
mysql> insert into departments_hive01 values(777, "Not known",1000);
mysql> insert into departments_hive01 values(8888, null,1000);
mysql> insert into departments_hive01 values(666, null,1100);
mysql> quit;
//Step 4. 4. Now import data from mysql table departments_hive01 to this hive table.
// Please make sure that data should be visible using below hive command. Also, while importing if null
//Also, while importing if null value found for department_name column replace it with "" (empty string) and for id column with -999 
//select * from departments_hive01;
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table departments_hive01 \
--hive-import \
--hive-overwrite \
--hive-table hadoopexam.departments_hive01 \
--null-string "" \
--null-non-string "-999"
--split-by "id" \
--fields-terminated-by "\001" \
--num-mappers 1

$ hive;
hive> use hadoopexam;
hive> select * from departments_hive01;
hive> quit;