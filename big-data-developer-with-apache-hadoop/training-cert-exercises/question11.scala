/**
 * Problem Scenario 15 : You have been given following mysql database details as well as other info.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Please accomplish following activities.
 * 1. In mysql departments table please insert following record. Insert into departments
 * values(9999, '"Data Science"');
 * 2. Now there is a downstream system which will process dumps of this file. However,
 * system is designed the way that it can process only files if fields are enlcosed in(') single
 * quote and separate of the field should be (-) and line needs to be terminated by : (colon).
 * 3. If data itself contains the " (double quote ) than it should be escaped by \.
 * 4. Please import the departments table in a directory called departments_enclosedby and
 * file should be able to process by downstream system.
 */
//step 1
$ mysql --user retail_dba -p cloudera
mysql> show databases;
mysql> use retail_db;
mysql> show tables;
mysql> INSERT INTO departments VALUES(9999, '"Data Science"');
mysql> commit;
mysql> SELECT * FROM departments;
mysql> exit;

//step 2
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba --password cloudera \
--table departments \
--delete-target-dir \
--target-dir departments_enclosedby \
--enclosed-by "\\'" \
--escaped-by "\\" \
--fields-terminated-by "-" \
--lines-terminated-by ":" \
--num-mappers 1

//Step 3 : Check the result. 
$ hdfs dfs -ls departments_enclosedby
$ hdfs dfs -cat departments_enclosedby/part-00000 