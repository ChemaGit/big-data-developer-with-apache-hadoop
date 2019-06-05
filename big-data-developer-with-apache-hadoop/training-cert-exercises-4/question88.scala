/** Question 88
 * Problem Scenario 11 : You have been given following mysql database details as well as other info.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Please accomplish following.
 * 1. Import departments table in a directory called departments.
 * 2. Once import is done, please insert following 5 records in departments mysql table.
 * mysql> Insert into departments values(16, 'Physics');
 * mysql> Insert into departments values(17, 'Chemistry');
 * mysql> Insert into departments values(18, 'Superb Maths');
 * mysql> Insert into departments values(19, 'Superb Science');
 * mysql> Insert into departments values(20, 'Computer Engineering');
 * 3. Now import only new inserted records and append to existring directory . which has been created in first step.
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1 : Clean already imported data. (In real exam, please make sure you dont delete data generated from previous exercise). 
$ hadoop fs -rm -R departments 

//Step 2 : Import data in departments directory. 
$ sqoop import \ 
--connect jdbc:mysql://quickstart:3306/retail_db \ 
--username retail_dba \ 
--password cloudera \ 
--table departments \ 
--target-dir /user/cloudera/departments 

//Step 3 : Insert the five records in departments table. 
$ mysql -user=retail_dba --password=cloudera 
mysql> use retail_db; 
mysql> Insert into departments values(10, "physics"); 
mysql> Insert into departments values(11, "Chemistry"); 
mysql> Insert into departments values(12, "Maths"); 
mysql> Insert into departments values(13, "Science"); 
mysql> Insert into departments values(14, "Engineering"); 
mysql> commit; 
mysql> select * from departments; 

//Step 4 : Get the maximum value of departments from last import, that should be 7
hdfs dfs -cat /user/cloudera/departments/part* that should be 7 

//Step 5 : Do the incremental import based on last import and append the results. 
sqoop import \ 
--connect "jdbc:mysql://quickstart.cloudera:330G/retail_db" \ 
--username retail_dba \ 
--password cloudera \ 
--table departments \ 
--target-dir /user/cloudera/departments \ 
--append \ 
--check-column "department_id" \ 
--incremental append \ 
--last-value 7 

//Step 6 : Now check the result. 
hdfs dfs -cat /user/cloudera/departments/part*