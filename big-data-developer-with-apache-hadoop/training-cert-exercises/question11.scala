/** Question 11
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
$ mysql -u root -p
mysql> use retail_db;
mysql> Insert into departments values(9999, '"Data Science"');
mysql> exit;

sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
  --username root \
  --password cloudera \
  --table departments \
  --as-textfile \
--enclosed-by "\\'" \
  --fields-terminated-by '-' \
  --lines-terminated-by ':' \
  --escaped-by "\\" \
  --delete-target-dir \
  --target-dir /user/cloudera/question11/departments_enclosedby \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hdfs dfs -ls /user/cloudera/question11/departments_enclosedby
$ hdfs dfs -cat /user/cloudera/question11/departments_enclosedby/part* | head -n 50