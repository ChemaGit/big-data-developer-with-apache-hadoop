/** Question 16
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
$ hive
  hive> CREATE DATABASE hadoopexam;
hive> use hadoopexam;
hive> CREATE TABLE departments(department_id int, department_name string);

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table departments \
  --columns "department_id,department_name" \
  --hive-import \
--hive-database hadoopexam \
--hive-table departments \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

hive> select * from departments;

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table departments \
  --columns "department_id,department_name" \
  --hive-import \
--hive-database hadoopexam \
--create-hive-table \
  --hive-table departments_new \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

hive> show tables;
hive> select * from departments_new;