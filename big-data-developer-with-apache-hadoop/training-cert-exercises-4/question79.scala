/**
 * Problem Scenario 18 : You have been given following mysql database details as well as other info. 
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Now accomplish following activities.
 * 1. Create mysql table as below.
 * mysql --user=retail_dba -password=cloudera
 * use retail_db;
 * CREATE TABLE IF NOT EXISTS departments_hive02(id int, department_name varchar(45), avg_salary int);
 * show tables;
 * 2. Now export data from hive table departments_hive01 in departments_hive02. 
 * While exporting, please note following. wherever there is a empty string it should be loaded as a null value in mysql.
 * wherever there is -999 value for int field, it should be created as null value.
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1 : Create table in mysql db as well. 
$ mysql -u retail_dba -p cloudera 
mysql> use retail_db; 
mysql> CREATE TABLE IF NOT EXISTS departments_hive02(id int, department_name varchar(45), avg_salary int); 
mysql> show tables; 

//Step 2 : Now export data from hive table to mysql table as per the requirement. 
$ sqoop export \
--connect jdbc:mysql://quickstart:3306/retail_db \ 
--username retaildba \ 
--password cloudera \ 
--table departments_hive02 \ 
--export-dir /user/hive/warehouse/departments_hive01 \ 
--input-fields-terminated-by '\001' \ 
--input-lines-terminated-by '\n' \ 
--num-mappers 1 \ 
--batch \ 
-input-null-string "" \ 
-input-null-non-string "-999" 

//step 3 : Now validate the data,
mysql> select * from departments_hive02;