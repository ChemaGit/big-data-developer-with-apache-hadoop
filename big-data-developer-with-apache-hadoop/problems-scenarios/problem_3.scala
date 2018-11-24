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

//1. Import all tables from mysql database into hdfs as avro data files. use compression and the compression codec should be snappy. data warehouse directory should be retail_stage.db
sqoop import-all-tables \
--connect jdbc:mysql://localhost/retail_db \
--username cloudera \
--password cloudera \
--warehose-dir /user/hive/warehouse/retail_stage.db \
--compress \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
--as-avrodatafile \
--autoreset-to-one-mapper

//2. Create a metastore table that should point to the orders data imported by sqoop job above. Name the table orders_sqoop.
$ hdfs dfs -get /user/hive/warehouse/retail_stage.db/orders/part-m-00000.avro
$ avro-tools getschema part-m-00000.avro > orders.avsc
$ hdfs dfs -mkdir /user/hive/schemas
$ hdfs dfs -mkdir /user/hive/schemas/order
$ hdfs dfs -put orders.avsc /user/hive/schemas/order

$ beeline -n cloudera -p cloudera -u jdbc:hive2://localhost:10000/default
hive> create external table orders_sqoop
STORED AS AVRO
LOCATION '/user/hive/warehouse/retail_stage.db/orders'
TBLPROPERTIES ('avro.schema.url'='/user/hive/schemas/order/orders.avsc')

//3. Write query in hive that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from orders_sqoop. 
select count(orderid) as MaxCount, order_date from orders_sqoop group by order_date order by MaxCount desc limit 1;

select * from orders_sqoop as X where X.order_date in (select inner.order_date from (select Y.order_date, count(1) as total_orders from orders_sqoop as Y group by Y.order_date order by total_orders desc, Y.order_date desc limit 1) inner);

//4. query table in impala that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from order_sqoop. 
Launch Impala shell by using command impala-shell

1. Run 'Invalidate metadata'
2. Run below query

select * from orders_sqoop as X where X.order_date in (select a.order_date from (select Y.order_date, count(1) as total_orders from orders_sqoop as Y group by Y.order_date order by total_orders desc, Y.order_date desc limit 1) a);

//5. Now create a table named retail.orders_avro in hive stored as avro, the table should have same table definition as order_sqoop. Additionally, this new table should be partitioned by the order month i.e -> year-order_month.(example: 2014-01)

create database retail;

create table orders_avro
    (order_id int,
    order_date date,
    order_customer_id int,
    order_status string)
    partitioned by (order_month string)
    STORED AS AVRO;

//6. Load data into orders_avro table from orders_sqoop table.
insert overwrite table orders_avro partition (order_month)
select order_id, to_date(from_unixtime(cast(order_date/1000 as int))), order_customer_id, order_status, substr(from_unixtime(cast(order_date/1000 as int)),1,7) as order_month from default.orders_sqoop;

//7. Write query in hive that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from orders_avro
select * from orders_avro as X where X.order_date in (select inner.order_date from (select Y.order_date, count(1) as total_orders from orders_avro as Y group by Y.order_date order by total_orders desc, Y.order_date desc limit 1) inner);

//8. evolve the avro schema related to orders_sqoop table by adding more fields named (order_style String, order_zone Integer)

$ hdfs dfs -get /user/hive/schemas/order/orders.avsc
$ gedit orders.avsc

3.{
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
  },{
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

//9. insert two more records into orders_sqoop table. 
insert into table orders_sqoop values (8888888,1374735600000,11567,"xyz",9,"CLOSED");
insert into table orders_sqoop values (8888889,1374735600000,11567,"xyz",9,"CLOSED");

//10. Write query in hive that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from orders_sqoop
select * from orders_sqoop as X where X.order_date in (select inner.order_date from (select Y.order_date, count(1) as total_orders from orders_sqoop as Y group by Y.order_date order by total_orders desc, Y.order_date desc limit 1) inner);

//11. query table in impala that shows all orders belonging to a certain day. This day is when the most orders were placed. select data from orders_sqoop
Launch Impala shell by using command impala-shell

1. Run 'Invalidate metadata'
2. Run below query

select * from orders_sqoop as X where X.order_date in (select a.order_date from (select Y.order_date, count(1) as total_orders from orders_sqoop as Y group by Y.order_date order by total_orders desc, Y.order_date desc limit 1) a);
