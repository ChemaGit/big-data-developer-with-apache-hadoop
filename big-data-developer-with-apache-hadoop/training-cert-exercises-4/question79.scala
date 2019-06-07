/** Question 79
  * Problem Scenario 18 : You have been given following mysql database details as well as other info.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Now accomplish following activities.
  * 1. Create mysql table as below.
  * mysql --user=retail_dba -password=cloudera
  * use retail_db;
  * CREATE TABLE IF NOT EXISTS departments_hive02(id int, department_name varchar(45), avg_salary int);
  * show tables;
  * 2. Now export data from hive table departments_hive01 in departments_hive02.
  * While exporting, please note following. wherever there is a empty string it should be loaded as a null value in mysql.
  * wherever there is -999 value for int field, it should be created as null value.
  */
$ mysql -u root -p
mysql> use hadoopexam;
mysql> CREATE TABLE IF NOT EXISTS departments_hive02(id int, department_name varchar(45), avg_salary int);
mysql> show tables;
mysql> exit;

$ hdfs dfs -ls /user/hive/warehouse/hadoopexam.db
$ hdfs dfs -ls /user/hive/warehouse/hadoopexam.db/departments_hive01
$ hdfs dfs -cat /user/hive/warehouse/hadoopexam.db/departments_hive01/part-m-00000

sqoop export \
--connect jdbc:mysql://quickstart:3306/hadoopexam \
  --username root \
  --password cloudera \
  --table departments_hive02 \
  --update-key "department_id" \
  --update-mode allowinsert \
--export-dir /user/hive/warehouse/hadoopexam.db/departments_hive01/ \
--input-fields-terminated-by '\001' \
  --input-lines-terminated-by '\n' \
  --input-null-string "-999" \
  --input-null-non-string -999 \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

$ mysql -u root -p
mysql> use hadoopexam;
mysql> select * from departments_hive02;
mysql> exit;