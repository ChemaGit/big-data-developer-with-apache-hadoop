/**
Problem 1:
1.Using sqoop, import orders table into hdfs to folders /user/cloudera/problem1/orders. File should be loaded as Avro File and use snappy compression
2.Using sqoop, import order_items  table into hdfs to folders /user/cloudera/problem1/order-items. Files should be loaded as avro file and use snappy compression
3.Using Spark Scala load data at /user/cloudera/problem1/orders and /user/cloudera/problem1/orders-items items as dataframes. 
4.Expected Intermediate Result: Order_Date , Order_status, total_orders, total_amount. In plain english, please find total orders and total amount per status per day. The result should be sorted by order date in descending, order status in ascending and total amount in descending and total orders in ascending. Aggregation should be done using below methods. However, sorting can be done using a dataframe or RDD. Perform aggregation in each of the following ways
a). Just by using Data Frames API - here order_date should be YYYY-MM-DD format
b). Using Spark SQL  - here order_date should be YYYY-MM-DD format
c). By using combineByKey function on RDDS -- No need of formatting order_date or total_amount
5.Store the result as parquet file into hdfs using gzip compression under folder
/user/cloudera/problem1/result4a-gzip
/user/cloudera/problem1/result4b-gzip
/user/cloudera/problem1/result4c-gzip
6.Store the result as parquet file into hdfs using snappy compression under folder
/user/cloudera/problem1/result4a-snappy
/user/cloudera/problem1/result4b-snappy
/user/cloudera/problem1/result4c-snappy
7.Store the result as CSV file into hdfs using No compression under folder
/user/cloudera/problem1/result4a-csv
/user/cloudera/problem1/result4b-csv
/user/cloudera/problem1/result4c-csv
8.create a mysql table named result and load data from /user/cloudera/problem1/result4a-csv to mysql table named result 
*/
//1.Using sqoop, import orders table into hdfs to folders /user/cloudera/problem1/orders. File should be loaded as Avro File and use snappy compression
sqoop import \
--connect jdbc:mysql://localhost/retail_db \
--username training \
--password training \
--table orders \
--as-avrodatafile \
--compress \
--compression-codec "org.apache.hadoop.io.compress.SnapyCodec" \
--delete-target-dir \
--target-dir /user/cloudera/problem1/orders \
--num-mappers 1

//2.Using sqoop, import order_items  table into hdfs to folders /user/cloudera/problem1/order-items. Files should be loaded as avro file and use snappy compression
sqoop import \
--connect jdbc:mysql://localhost/retail_db \
--username training \
--password training \
--table order_items \
--as-avrodatafile \
--compress \
--compression-codec org.apache.hadoop.io.compress.SnapyCodec \
--delete-target-dir \
--target-dir /user/cloudera/problem1/order-items \
--num-mappers 1

//3.Using Spark Scala load data at /user/cloudera/problem1/orders and /user/cloudera/problem1/orders-items items as dataframes. 
import com.databricks.spark.avro._;
import org.apache.avro._
val ordersDF = sqlContext.read.avro("/user/cloudera/problem1/orders");
val orderItemDF = sqlContext.read.avro("/user/cloudera/problem1/order-items");

/*
4.Expected Intermediate Result: Order_Date , Order_status, total_orders, total_amount. In plain english, please find total orders and total amount per status per day. 
The result should be sorted by order date in descending, order status in ascending and total amount in descending and total orders in ascending. 
Aggregation should be done using below methods. However, sorting can be done using a dataframe or RDD. Perform aggregation in each of the following ways
a). Just by using Data Frames API - here order_date should be YYYY-MM-DD format
b). Using Spark SQL  - here order_date should be YYYY-MM-DD format
c). By using combineByKey function on RDDS -- No need of formatting order_date or total_amount
*/
val joinedOrderDataDF = ordersDF.join(orderItemDF,ordersDF("order_id")===orderItemDF("order_item_order_id"))

//a). Just by using Data Frames API - here order_date should be YYYY-MM-DD format
import org.apache.spark.sql.functions._

val dataFrameResult = joinedOrderDataDF.groupBy(to_date(from_unixtime(col("order_date")/1000)).alias("order_formatted_date"),
col("order_status")).agg(round(sum("order_item_subtotal"),2).alias("total_amount"),
countDistinct("order_id").alias("total_orders")).orderBy(col("order_formatted_date").desc,
col("order_status"),
col("total_amount").desc,
col("total_orders"))

dataFrameResult.show();

//b). Using Spark SQL  - here order_date should be YYYY-MM-DD format
joinedOrderDataDF.registerTempTable("order_joined")

var sqlResult = sqlContext.sql("select to_date(from_unixtime(cast(order_date/1000 as bigint))) as order_formatted_date, order_status, cast(sum(order_item_subtotal) as DECIMAL (10,2)) as total_amount, count(distinct(order_id)) as total_orders from order_joined group by to_date(from_unixtime(cast(order_date/1000 as bigint))), order_status order by order_formatted_date desc,order_status,total_amount desc, total_orders")

sqlResult.show()

//c). By using combineByKey function on RDDS -- No need of formatting order_date or total_amount
var comByKeyResult = joinedOrderDataDF.map(x=> ((x(1).toString,x(3).toString),(x(8).toString.toFloat,x(0).toString))).
combineByKey((x:(Float, String))=>(x._1,Set(x._2)),(x:(Float,Set[String]),y:(Float,String))=>(x._1 + y._1,x._2+y._2),(x:(Float,Set[String]),y:(Float,Set[String]))=>(x._1+y._1,x._2++y._2)).
map(x=> (x._1._1,x._1._2,x._2._1,x._2._2.size)).toDF().orderBy(col("_1").desc,col("_2"),col("_3").desc,col("_4"));

comByKeyResult.show();

//5.Store the result as parquet file into hdfs using gzip compression under folder
sqlContext.setConf("spark.sql.parquet.compression.codec","gzip");

// /user/cloudera/problem1/result4a-gzip
dataFrameResult.write.parquet("/user/cloudera/problem1/result4a-gzip");

// /user/cloudera/problem1/result4b-gzip
sqlResult.write.parquet("/user/cloudera/problem1/result4b-gzip");

// /user/cloudera/problem1/result4c-gzip
comByKeyResult.write.parquet("/user/cloudera/problem1/result4c-gzip");

//6.Store the result as parquet file into hdfs using snappy compression under folder
sqlContext.setConf("spark.sql.parquet.compression.codec","snappy");

// /user/cloudera/problem1/result4a-snappy
dataFrameResult.write.parquet("/user/cloudera/problem1/result4a-snappy");

// /user/cloudera/problem1/result4b-snappy
sqlResult.write.parquet("/user/cloudera/problem1/result4b-snappy");

// /user/cloudera/problem1/result4c-snappy
comByKeyResult.write.parquet("/user/cloudera/problem1/result4c-snappy");

//7.Store the result as CSV file into hdfs using No compression under folder
// /user/cloudera/problem1/result4a-csv
dataFrameResult.map(x=> x(0) + "," + x(1) + "," + x(2) + "," + x(3)).saveAsTextFile("/user/cloudera/problem1/result4a-csv")

// /user/cloudera/problem1/result4b-csv
sqlResult.map(x=> x(0) + "," + x(1) + "," + x(2) + "," + x(3)).saveAsTextFile("/user/cloudera/problem1/result4b-csv")

// /user/cloudera/problem1/result4c-csv
comByKeyResult.map(x=> x(0) + "," + x(1) + "," + x(2) + "," + x(3)).saveAsTextFile("/user/cloudera/problem1/result4c-csv")

//8.create a mysql table named result and load data from /user/cloudera/problem1/result4a-csv to mysql table named result 
mysql> CREATE TABLE retail_db.result(order_date varchar(255) not null,order_status varchar(255) not null, total_orders int, total_amount numeric, CONSTRAINT pk_order_result PRIMARY KEY (order_date,order_status));

sqoop export \
--connect jdbc:mysql://localhost/retail_db \
--username training \
--password training \
--table result \
--export-dir /user/cloudera/problem1/result4a-csv \
--columns "order_date,order_status,total_amount,total_orders" \
--num-mappers 1