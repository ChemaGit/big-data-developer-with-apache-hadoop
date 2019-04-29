/*
Question 4: Correct
PreRequiste:
Import all table into Hdfs using below sqoop command in avro format:

sqoop import-all-tables \
--connect "jdbc:mysql://localhost/retail_db" \
--username root \
--password cloudera \
--warehouse-dir "/user/hive/warehouse/retail_db.db" \
--compress \
--compression-codec snappy \
--as-avrodatafile \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--autoreset-to-one-mapper

Instructions
Create avro schema file from order table
Get avro schema

hdfs dfs -get /user/hive/warehouse/retail_db.db/orders/part-m-00000.avro
avro-tools getschema part-m-00000.avro > orders.avsc
hdfs dfs -mkdir /user/hive/schemas
hdfs dfs –put orders.avsc /user/hive/schemas

Instructions:
Create a metastore table that should point to above avro schema, Name the table orders_sqoop

Output Requirement:
Result should be saved in orders_sqoop
*/
sqoop import-all-tables \
  --connect "jdbc:mysql://localhost/retail_db" \
  --username root \
  --password cloudera \
  --warehouse-dir "/user/hive/warehouse/retail_db.db" \
  --compress \
--compression-codec snappy \
--as-avrodatafile \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--autoreset-to-one-mapper

$ hdfs dfs -ls /user/hive/warehouse/retail_db.db/
  $ hdfs dfs -get /user/hive/warehouse/retail_db.db/orders/part-m-00000.avro /home/cloudera/avro
$ avro-tools getschema /home/cloudera/avro/part-m-00000.avro > /home/cloudera/avro/orders.avsc
$ hdfs dfs -mkdir /user/hive/schemas
$ hdfs dfs -put /home/cloudera/avro/orders.avsc /user/hive/schemas

$ hive
  hive> create table orders_sqoop STORED AS AVRO LOCATION '/user/hive/warehouse/retail_db.db/orders/' TBLPROPERTIES('avro.schema.url'='/user/hive/schemas/orders.avsc');
hive> describe formatted orders_sqoop;
hive> select * from orders_sqoop limit 10;