/** Question 85
 * Problem Scenario 14 : You have been given following mysql database details as well as other info.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Please accomplish following activities.
 * 1. Create a csv file named updated_departments.csv with the following contents in local file system.
 * updated_departments.csv
 * 2,fitness
 * 3,footwear
 * 12,fathematics
 * 13,fcience
 * 14,engineering
 * 1000,management
 * 2. Upload this csv file to hdfs filesystem,
 * 3. Now export this data from hdfs to mysql retaildb.departments table. During upload make sure existing department will just updated and new departments needs to be inserted.
 * 4. Now update updated_departments.csv file with below content.
 * 2,Fitness
 * 3,Footwear
 * 12,Fathematics
 * 13,Science
 * 14,Engineering
 * 1000,Management
 * 2000,Quality Check
 * 5. Now upload this file to hdfs.
 * 6. Now export this data from hdfs to mysql retail_db.departments table. During upload make sure existing department will just updated and no new departments needs to be inserted.
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1 : Create a csv tile named updated_departments.csv with give content.
$ gedit updated_departments.csv & 

//Step 2 : Now upload this tile to HDFS. Create a directory called newdata. 
$ hdfs dfs -mkdir new_data 
$ hdfs dfs -put updated_departments.csv newdata/ 

//Step 3 : Check whether tile is uploaded or not. 
$ hdfs dfs -ls new_data
$ hdfs dfs -cat newdata/updated_departments.csv 

//Step 4 : Export this file to departments table using sqoop. 
$ sqoop export \
--connect jdbc:mysql://quickstart:3306/retail_db \ 
--username retail_dba \ 
--password cloudera \ 
--table departments \ 
--export-dir new_data \ 
--batch \ 
--num-mappers 1 \ 
--update-key department_id \ 
--update-mode allowinsert 

//Step 5 : Check whether required data upsert is done or not. 
$ mysql -u retail_dba -p cloudera 
mysql> show databases; 
mysql> use retail db; 
mysql> show tables; 
mysql> select * from departments;

//Step 6 : Update updated_departments.csv file. 
$ gedit updated_departments.csv &

//Step 7 : Override the existing file in hdfs.
$ hdfs dfs -rm newdata/updated_departments.csv  
$ hdfs dfs -put updated_departments.csv newdata/ 
$ hdfs dfs -cat newdata/updated_departments.csv

//Step 8 : Now do the Sqoop export as per the requirement. 
$ sqoop export \
--connect jdbc:mysql://quickstart:3306/retail_db \ 
--username retail_dba \ 
--password cloudera \ 
--table departments \ 
--export-dir new_data \ 
--batch \ 
--num-mappers 1 \ 
--update-key department_id \ 
--update-mode updateonly 

//Step 9 : Check whether required data update is done or not. 
$ mysql -u retail_dba -p cloudera 
mysql> show databases; 
mysql> use retail db; 
mysql> show tables; 
mysql> select * from departments;