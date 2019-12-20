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

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table departments \
  --fields-terminated-by '|' \
  --lines-terminated-by '\n' \
  --delete-target-dir \
  --target-dir /user/cloudera/question67/departments \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table departments \
  --fields-terminated-by '|' \
  --lines-terminated-by '\n' \
  --incremental append \
  --check-column "department_id" \
  --target-dir /user/cloudera/question67/departments \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hdfs dfs -ls /user/cloudera/question67/departments
$ hdfs dfs -cat /user/cloudera/question67/departments/part*