/** Question 58
 * Problem Scenario 5 : You have been given following mysql database details.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Please accomplish following activities.
 * 1. List all the tables using sqoop command from retail_db
 * 2. Write simple sqoop eval command to check whether you have permission to read database tables or not.
 * 3. Import all the tables as avro files in /user/hive/warehouse/retail cca174.db
 * 4. Import departments table as a text file in /user/cloudera/departments.
 */

//Step 1: List all the tables using sqoop command from retail_db
sqoop list-tables \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera

//Step 2: Write simple sqoop eval command to check whether you have permission to read database tables or not.
sqoop eval \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera
--query "SELECT * FROM departments"

//Step 3: Import all the tables as avro files in /user/hive/warehouse/retail_cca174.db
sqoop import-all-tables \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--as-avrodatafile \
--warehouse-dir /user/hive/warehouse/retail_cca174.db \
--autoreset-to-one-mapper

//Step 4: Import departments table as a text file in /user/cloudera/departments.
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table departments \
--delete-target-dir \
--target-dir /user/cloudera/departments \
--num-mappers 1

//Step 5 : Verify the imported data. 
hdfs dfs -ls /user/cloudera/departments 
hdfs dfs -ls /user/hive/warehouse/retail_cca174
hdfs dfs -ls /user/hive/warehouse/retail_cca174/products