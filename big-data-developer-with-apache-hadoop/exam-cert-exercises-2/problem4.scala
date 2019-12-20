/*
Question 4: Correct
PreRequiste:
[Prerequisite section will not be there in actual exam]
Create product_hive table in mysql using below script:
use retail_db;
create table product_hive as select * from products;
truncate product_hive;

Instructions:
Using sqoop export all data from metastore product_new table created in last problem statement into products_hive table table in mysql.

Data Description:
A mysql instance is running on the gateway node.In that instance you will find customers table that contains customers data.
> Installation : on the cluser node gateway
> Database name: retail_db
> Table name: product_hive
> Username: root
> Password: cloudera

Output Requirement:
product_hive table should contain all product data imported from hive table.
*/
$ mysql -u root -p
mysql> use retail_db;
mysql> create table product_hive as select * from products;
mysql> truncate product_hive;

sqoop export \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table product_hive \
  --hcatalog-database default \
--hcatalog-table product_new \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

mysql> select * from product_hive limit 10;
mysql> exit;