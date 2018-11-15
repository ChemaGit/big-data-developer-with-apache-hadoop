/** Question 67
 * Problem Scenario 9 : You have been given following mysql database details as well as other info.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Please accomplish following.
 * 1. Import departments table in a directory.
 * 2. Again import departments table same directory (However, directory already exist hence it should not overrride and append the results)
 * 3. Also make sure your results fields are terminated by '|' and lines terminated by '\n\
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solutions : 
//Step 1 : Clean the hdfs file system, if they exists clean out. 
$ hadoop fs -rm -R departments 
$ hadoop fs -rm -R categories 
$ hadoop fs -rm -R products 
$ hadoop fs -rm -R orders 
$ hadoop fs -rm -R order_items 
$ hadoop fs -rm -R customers 

//Step 2 : Now import the department table as per requirement. 
sqoop import \ 
--connect jdbc:mysql://quickstart:3306/retail_db \ 
--username retail_dba \ 
--password cloudera \ 
--table departments \ 
--delete-target-dir \
--target-dir=departments \ 
--fields-terminated-by '|' \ 
--lines-terminated-by '\n' \ 
--m 1  

//Step 3 : Check imported data. 
$ hdfs dfs -ls departments 
$ hdfs dfs -cat departments/part-m-00000 

//Step 4 : Now again import data and needs to appended. 
sqoop import \ 
--connect jdbc:mysql://quickstart:3306/retail_db \ 
--username retail_dba \ 
--password cloudera \ 
--table departments \ 
--target-dir departments \ 
--append \ 
--fields-terminated-by '|' \ 
--lines-terminated-by '\n' \ 
--m 1 

//Step 5 : Again Check the results 
$ hdfs dfs -ls departments 
$ hdfs dfs -cat departments/*