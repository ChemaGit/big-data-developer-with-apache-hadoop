/** Question 81
 * Problem Scenario 7 : You have been given following mysql database details as well as other info.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Please accomplish following.
 * 1. Import department tables using your custom boundary query, which import departments between 1 to 25.
 * 2. Also make sure each tables file is partitioned in 2 files e.g. part-00000, part-00002
 * 3. Also make sure you have imported only two columns from table, which are department_id,department_name
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solutions : 
//Step 1 : Clean the hdfs tile system, if they exists clean out. 
$ hadoop fs -rm -R departments 
$ hadoop fs -rm -R categories 
$ hadoop fs -rm -R products 
$ hadoop fs -rm -R orders 
$ hadoop fs -rm -R order_itmes 
$ hadoop fs -rm -R customers 

//Step 2 : Now import the department table as per requirement. 
$ sqoop import \ 
--connect jdbc:mysql://quickstart:3306/retail_db \ 
--username retail_dba \ 
--password cloudera \ 
--table departments \ 
--target-dir /user/cloudera/departments \ 
-m2 \ 
--boundary-query "select 1, 25 from departments" \ 
--columns department_id,department_name 

//Step 3 : Check imported data. 
$ hdfs dfs -ls departments 
$ hdfs dfs -cat departments/part-m-00000 
$ hdfs dfs -cat departments/part-m-00001