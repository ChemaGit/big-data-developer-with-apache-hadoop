/** Question 12
  * Problem Scenario 12 : You have been given following mysql database details as well as other info.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Please accomplish following.
  * 1. Create a table in retail_db with following definition.
  * CREATE table departments_new (department_id int(11), department_name varchar(45),created_date TIMESTAMP DEFAULT NOW());
  * 2. Now insert records from departments table to departments_new
  * 3. Now import data from departments_new table to hdfs.
  * 4. Insert following 5 records in department_new table.
  * Insert into departments_new values(110, "Civil" , null);
  * Insert into departments_new values(111, "Mechanical" , null);
  * Insert into departments_new values(112, "Automobile" , null);
  * Insert into departments_new values(113, "Pharma" , null);
  * Insert into departments_new values(114, "Social Engineering" , null);
  * 5. Now do the incremental import based on created_date column.
  */
$ mysql -u root -p
mysql> CREATE table departments_new (department_id int(11), department_name varchar(45),created_date TIMESTAMP DEFAULT NOW());
mysql> INSERT INTO  departments_new SELECT department_id,department_name, null FROM departments;

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table departments_new \
  --delete-target-dir \
  --target-dir /user/cloudera/question12/departments_new \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hdfs dfs -ls /user/cloudera/question12/departments_new
$ hdfs dfs -cat /user/cloudera/question12/departments_new/part*

Insert into departments_new values(110, "Civil" , null);
Insert into departments_new values(111, "Mechanical" , null);
Insert into departments_new values(112, "Automobile" , null);
Insert into departments_new values(113, "Pharma" , null);
Insert into departments_new values(114, "Social Engineering" , null);

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table departments_new \
  --incremental "append" \
  --check-column "created_date" \
  --last-value "2019-05-09 14:39:44" \
  --target-dir  /user/cloudera/question12/departments_new \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hdfs dfs -ls /user/cloudera/question12/departments_new
$ hdfs dfs -cat /user/cloudera/question12/departments_new/part*