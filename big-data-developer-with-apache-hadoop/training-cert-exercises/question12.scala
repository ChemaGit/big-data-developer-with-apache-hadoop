/**
 * Problem Scenario 12 : You have been given following mysql database details as well as other info.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Please accomplish following.
 * 1. Create a table in retail_db with following definition.
 * CREATE table departments_new (department_id int(11), department_name varchar(45),created_date T1MESTAMP DEFAULT NOW());
 * 2. Now insert records from departments table to departments_new
 * 3. Now import data from departments_new table to hdfs.
 * 4. Insert following 5 records in department_new table. 
 * Insert into departments_new values(110, "Civil" , null); 
 * Insert into departments_new values(111, "Mechanical" , null);
 * Insert into departments_new values(112, "Automobile" , null); 
 * Insert into departments_new values(113, "Pharma" , null);
 * Insert into departments_new values(114, "Social Engineering" , null);
 * 5. Now do the incremental import based on created_date column.
 */
//1. Create a table in retail_db with following definition.
$ mysql -u retail_dba -p cloudera
mysql> show databases;
mysql> use retail_db
mysql> CREATE table departments_new (department_id int(11), department_name varchar(45),created_date T1MESTAMP DEFAULT NOW()); 
//2. Now insert records from departments table to departments_new
mysql> INSERT INTO departments_new SELECT * FROM departments;
mysql> select * from departments_new;
//3. Now import data from departments_new table to hdfs.
sqoop import \
--connnect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba --password cloudera \
--table departments_new \
--warehouse-dir departments_new \
--num-mappers 1
$ hdfs dfs -ls /user/cloudera/departments_new/departments_new
$ hdfs dfs -cat /user/cloudera/departments_new/departments_new/part-00000 
//4. Insert following 5 records in department_new table. 
mysql> source /home/training/Desktop/ExercisesCert/Sqoop/new_records.sql
mysql> select * from departments_new;
//5. Now do the incremental import based on created_date column.
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba --password cloudera \
--table departments_new \
--check-column created_date \
--incremental append \
--last-value "2018-10-22 10:49:24" \
--warehouse-dir departments_new \
--num-mappers 1

$ hdfs dfs -ls /user/cloudera/departments_new/departments_new
$ hdfs dfs -cat /user/cloudera/departments_new/departments_new/part-00000 