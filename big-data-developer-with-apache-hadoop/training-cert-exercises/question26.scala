/** Question 26
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
$ hive
  hive> use hadoopexam;
hive> create table departments_hive(department_id int, department_name string);

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table departments \
  --as-textfile \
  --hive-import \
--hive-database hadoopexam \
--hive-table departments_hive \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

hive> select * from departments_hive;
hive> exit;