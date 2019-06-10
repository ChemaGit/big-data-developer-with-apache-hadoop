/** Question 88
  * Problem Scenario 11 : You have been given following mysql database details as well as other info.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Please accomplish following.
  * 1. Import departments table in a directory called /user/cloudera/question88/departments.
  * 2. Once import is done, please insert following 5 records in departments mysql table.
  * mysql> Insert into departments values(16, 'Physics');
  * mysql> Insert into departments values(17, 'Chemistry');
  * mysql> Insert into departments values(18, 'Superb Maths');
  * mysql> Insert into departments values(19, 'Superb Science');
  * mysql> Insert into departments values(20, 'Computer Engineering');
  * 3. Now import only new inserted records and append to existring directory . which has been created in first step.
  */
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table departments \
  --delete-target-dir \
  --target-dir /user/cloudera/question88/departments \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

$ hdfs dfs -cat /user/cloudera/question88/departments/part*

$ mysql -u root -p
mysql> use retail_db;
mysql> Insert into departments values(16, 'Physics');
mysql> Insert into departments values(17, 'Chemistry');
mysql> Insert into departments values(18, 'Superb Maths');
mysql> Insert into departments values(19, 'Superb Science');
mysql> Insert into departments values(20, 'Computer Engineering');
mysql> exit;

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table departments \
  --incremental append \
  --check-column "department_id" \
  --last-value 10005 \
  --target-dir /user/cloudera/question88/departments \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

$ hdfs dfs -cat /user/cloudera/question88/departments/part*