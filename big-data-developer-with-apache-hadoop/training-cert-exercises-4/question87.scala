/** Question 87
 * Problem Scenario 20 : You have been given MySQL DB with following details.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * table=retail_db.categories
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Please accomplish following activities.
 * 1. Write a Sqoop Job which will import "retaildb.categories" table to hdfs, in a directory name "categories_targetJob".
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1 : Connecting to existing MySQL Database 
$ mysql -u retail_dba -p cloudera 
mysql> use retail_db; 

Step 2 : Show all the available tables 
mysql> show tables; 

//Step 3 : Below is the command to create Sqoop Job (Please note that - import space is mandatory) 
$ sqoop job \ 
--create sqoopjob \ 
-- import \ 
--connect "jdbc:mysql://quickstart:3306/retail_db" \ 
--username retail_dba \ 
--password cloudera \ 
--table categories \ 
--target-dir categories_targetJob \ 
--lines-terminated-by '\n' 

//Step 4 : List all the Sqoop Jobs 
$ sqoop job --list 

//Step 5 : Show details of the Sqoop Job 
$ sqoop job --show sqoopjob 

//Step 6 : Execute the sqoopjob 
$ sqoopjob --exec sqoopjob 

//Step 7 : Check the output of import job 
$ hdfs dfs -ls categories_target_job 
$ hdfs dfs -cat categories_target_job/part*