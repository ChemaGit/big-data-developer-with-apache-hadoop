/** Question 24
  * Problem Scenario 13 : You have been given following mysql database details as well as other info.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Please accomplish following.
  * 1. Create a table in retail_db with following definition.
  * CREATE table departments_export (department_id int(11), department_name varchar(45), created_date TIMESTAMP DEFAULT NOW());
  * 2. Now import the data from following directory into departments_export table,
  * /user/cloudera/question12/departments_new
  */

$ mysql -u root -p
mysql> CREATE table departments_export(department_id int(11), department_name varchar(45), created_date TIMESTAMP DEFAULT NOW());

sqoop export \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table departments_export \
  --export-dir /user/cloudera/question12/departments_new \
  --update-key "department_id" \
  --update-mode allowinsert \
--input-fields-terminated-by ',' \
  --input-lines-terminated-by '\n' \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

mysql> SELECT * FROM departments_export LIMIT 10;
mysql> exit;