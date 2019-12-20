/*
Question 2: Correct
PreRequiste:
[Prerequisite section will not be there in actual exam]

Run below sqoop command to import customers table from mysql into hdfs to the destination /user/cloudera/problem1/customers/text2
sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--username root \
--password cloudera \
--table customers \
--target-dir /user/cloudera/problem1/customers/text2 \
--fields-terminated-by "^" \
--columns "customer_id,customer_fname,customer_city"

Create customer_new table in mysql using below script:
create table retail_db.customer_new(id int,lname varchar(255),city varchar(255));

Instructions

Using sqoop export all data back from hdfs location "/user/cloudera/problem1/customers/text2" into customers_new table in mysql.

Data Description:
A mysql instance is running on the gateway node.In that instance you will find customers table that contains customers data.

> Installation : on the cluser node gateway
> Database name: retail_db
> Table name: customer_new
> Username: root
> Password: cloudera

Output Requirement:
customer_new table should contain all customers from HDFS location.
*/

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
  --username root \
  --password cloudera \
  --table customers \
  --delete-target-dir \
  --target-dir /user/cloudera/problem1/customers/text2 \
  --fields-terminated-by "^" \
  --columns "customer_id,customer_fname,customer_city" \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ mysql -u root -p cloudera
  mysql> create table retail_db.customer_new(id int,lname varchar(255),city varchar(255));

sqoop export \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table customer_new \
  --export-dir /user/cloudera/problem1/customers/text2 \
  --update-key "customer_id" \
  --update-mode allowinsert \
--input-fields-terminated-by "^" \
  --input-lines-terminated-by '\n' \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

mysql> use retail_db;
mysql> select * from customer_new limit 20;
mysql> exit;