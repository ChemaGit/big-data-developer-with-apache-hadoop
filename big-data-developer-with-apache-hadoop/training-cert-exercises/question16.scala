/**
 * Problem Scenario 10 : You have been given following mysql database details as well as
 * other info.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Please accomplish following.
 * 1. Create a database named hadoopexam and then create a table named departments in it, with following fields. department_id int, department_name string 
 * e.g. location should be hdfs://quickstart.cloudera:8020/user/hive/warehouse/hadoopexam.db/departments
 * 2. Please import data in existing table created above from retaidb.departments into hive table hadoopexam.departments.
 * 3. Please import data in a non-existing table, means while importing create hive table named hadoopexam.departments_new
 */
//Step 1
$ hive
> create database hadoopexam;
> show databases;
> use hadoopexam;
> CREATE TABLE IF NOT EXISTS hadoopexam.departments(department_id INT, department_name STRING);
> show tables;
> quit;
//Step 2 Please import data in existing table created above from retaidb.departments into hive table hadoopexam.departments.
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba  --password cloudera \
--table departments \
--columns "department_id, department_name" \
--hive-import \
--hive-overwrite \
--hive-table hadoopexam.departments \
--num-mappers 1

$ hive
> use hadoopexam;
> select * from departments limit 5;
> quit;

//Step 3 Please import data in a non-existing table, means while importing create hive table named hadoopexam.departments_new
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba  --password cloudera \
--table departments \
--columns "department_id, department_name" \
--hive-import \
--hive-overwrite \
--hive-table hadoopexam.departments_new \
--num-mappers 1

$ hive
> use hadoopexam;
> select * from departments_new limit 5;
> desc formatted departments_new;
> quit;