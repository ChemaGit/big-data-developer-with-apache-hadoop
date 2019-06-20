/**
Problem 3: Perform in the same sequence

1. Import all tables from mysql database into hdfs as avro data files. use compression and the compression codec should be snappy. data warehouse directory should be retail_stage.db
2. Create a metastore table that should point to the orders data imported by sqoop job above. Name the table orders_sqoop. 
3. Write query in hive that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from orders_sqoop. 
4. query table in impala that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from order_sqoop. 
5. Now create a table named retail.orders_avro in hive stored as avro, the table should have same table definition as order_sqoop. Additionally, this new table should be partitioned by the order month i.e -> year-order_month.(example: 2014-01)
6. Load data into orders_avro table from orders_sqoop table.
7. Write query in hive that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from orders_avro
8. evolve the avro schema related to orders_sqoop table by adding more fields named (order_style String, order_zone Integer)
9. insert two more records into orders_sqoop table. 
10. Write query in hive that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from orders_sqoop
11. query table in impala that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from orders_sqoop
  */

// 1. Import all tables from mysql database into hdfs as avro data files. use compression and the compression codec should be snappy. data warehouse directory should be retail_stage.db
sqoop import-all-tables \
  --connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --as-avrodatafile \
  --compress \
--compression-codec snappy \
--warehouse-dir /user/hive/warehouse/retail_stage.db \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--autoreset-to-one-mapper

// 2. Create a metastore table that should point to the orders data imported by sqoop job above. Name the table orders_sqoop.

$ avro-tools getschema hdfs://quickstart.cloudera/user/hive/warehouse/retail_stage.db/orders/part-m-00000.avro >> /home/cloudera/files/orders_sqoop.avsc
$ hdfs dfs -mkdir /user/hive/schemas
$ hdfs dfs -put /home/cloudera/files/orders_sqoop.avsc /user/hive/schemas
$ hive
  hive> use default;
hive> CREATE EXTERNAL TABLE orders_sqoop(order_id int,order_date bigint,order_customer_id int,order_status string) STORED AS AVRO LOCATION '/user/hive/warehouse/retail_stage.db/orders' TBLPROPERTIES('avro.compression'='snappy', avro.schema.url='/user/hive/schemas/orders_sqoop.avsc');
hive> select * from orders_sqoop limit 10;

// 3. Write query in hive that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from orders_sqoop.
hive> select sub.order_date, os.order_id,os.order_status,os.order_customer_id from (select order_date,count(order_id) as total_orders from orders_sqoop group by order_date order by total_orders desc limit 1) sub join orders_sqoop os on(sub.order_date = os.order_date);

// 4. query table in impala that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from order_sqoop.
$ impala-shell
> invalidate metadata;
> use default;
> show tables;
> select sub.order_date, os.order_id,os.order_status,os.order_customer_id from (select order_date,count(order_id) as total_orders from orders_sqoop group by order_date order by total_orders desc limit 1) sub join orders_sqoop os on(sub.order_date = os.order_date);

// 5. Now create a table named retail.orders_avro in hive stored as avro, the table should have same table definition as order_sqoop. Additionally, this new table should be partitioned by the order month i.e -> year-order_month.(example: 2014-01)

$ hive
  hive> create database retail;
hive> use retail;
hive> create table orders_avro(order_id int,order_date date,order_customer_id int,order_status string)partitioned by (order_month string)STORED AS AVRO;

// 6. Load data into orders_avro table from orders_sqoop table.
hive> set hive.exec.dynamic.partition.mode=nonstrict;
hive> insert overwrite table orders_avro partition (order_month) select order_id, to_date(from_unixtime(cast(order_date/1000 as int))), order_customer_id, order_status, substr(from_unixtime(cast(order_date/1000 as int)),1,7) as order_month from default.orders_sqoop;

// 7. Write query in hive that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from orders_avro
hive> select sub.order_date, os.order_id,os.order_status,os.order_customer_id,os.order_month from (select order_date,count(order_id) as total_orders from orders_avro group by order_date order by total_orders desc limit 1) sub join orders_avro os on(sub.order_date = os.order_date);

// 8. evolve the avro schema related to orders_sqoop table by adding more fields named (order_style String, order_zone Integer)
$ hdfs dfs -ls /user/hive/warehouse/retail_stage.db/orders
$ avro-tools getschema hdfs://quickstart.cloudera/user/hive/warehouse/retail_stage.db/orders/part-m-00000.avro >> /home/cloudera/files/orders_sqoop.avsc
$ gedit /home/cloudera/files/orders_sqoop.avsc &

{
  "type" : "record",
  "name" : "orders",
  "doc" : "Sqoop import of orders",
  "fields" : [ {
  "name" : "order_id",
  "type" : [ "null", "int" ],
  "default" : null,
  "columnName" : "order_id",
  "sqlType" : "4"
}, {
  "name" : "order_date",
  "type" : [ "null", "long" ],
  "default" : null,
  "columnName" : "order_date",
  "sqlType" : "93"
}, {
  "name" : "order_customer_id",
  "type" : [ "null", "int" ],
  "default" : null,
  "columnName" : "order_customer_id",
  "sqlType" : "4"
}, {
  "name" : "order_style",
  "type" : [ "null", "string" ],
  "default" : null,
  "columnName" : "order_style",
  "sqlType" : "12"
}, {
  "name" : "order_zone",
  "type" : [ "null", "int" ],
  "default" : null,
  "columnName" : "order_zone",
  "sqlType" : "4"
}, {
  "name" : "order_status",
  "type" : [ "null", "string" ],
  "default" : null,
  "columnName" : "order_status",
  "sqlType" : "12"
} ],
  "tableName" : "orders"
}

$ hdfs dfs -put -f /home/cloudera/files/orders_sqoop.avsc /user/hive/schemas

// 9. insert two more records into orders_sqoop table.
hive> insert into table orders_sqoop values (8888888,1374735600000,11567,"xyz",9,"CLOSED");
hive> insert into table orders_sqoop values (8888889,1374735600000,11567,"xyz",9,"CLOSED");

// 10. Write query in hive that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from orders_sqoop
hive> select sub.order_date, os.order_id,os.order_status,os.order_customer_id from (select order_date,count(order_id) as total_orders from orders_sqoop group by order_date order by total_orders desc limit 1) sub join orders_sqoop os on(sub.order_date = os.order_date);
// 11. query table in impala that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from orders_sqoop
$ impala-shell
> invalidate metadata;
> use default;
> show tables;
> select sub.order_date, os.order_id,os.order_status,os.order_customer_id from (select order_date,count(order_id) as total_orders from orders_sqoop group by order_date order by total_orders desc limit 1) sub join orders_sqoop os on(sub.order_date = os.order_date);