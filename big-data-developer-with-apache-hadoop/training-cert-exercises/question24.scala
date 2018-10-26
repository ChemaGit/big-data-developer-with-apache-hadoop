/**
 * Problem Scenario 13 : You have been given following mysql database details as well as other info.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Please accomplish following.
 * 1. Create a table in retailedb with following definition.
 * CREATE table departments_export (department_id int(11), department_name varchar(45), created_date TIMESTAMP DEFAULT NOW());
 * 2. Now import the data from following directory into departments_export table: /user/cloudera/departments_new
 */
//Step 1. Create a table in retailedb with following definition.
$ mysql -u retail_dba -p cloudera
mysql> use retail_db;
mysql> CREATE table departments_export (department_id int(11) null, department_name varchar(45) null, created_date TIMESTAMP DEFAULT NOW());
mysql> show tables;
mysql> commit;
mysql> quit;

//Step 2. Now import the data from following directory into departments_export table: /user/cloudera/departments_new
sqoop export \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table departments_export \
--export-dir /user/cloudera/departments_new \
--num-mappers 1 \
--batch

//Step 3. Check the results
$ mysql -u retail_dba -p cloudera
mysql> use retail_db;
mysql> select * from departments_export;
mysql> quit;