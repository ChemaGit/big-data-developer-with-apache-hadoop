/*
Question 5: Correct
PreRequiste:
[PreRequiste will not be there in actual exam]
Run below sqoop command to import orders table from mysql into hdfs to the destination /user/cloudera/problem3/parquet as parquet file.

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table orders \
--as-parquetfile \
--target-dir /user/cloudera/problem3/parquet

Instructions:
Fetch all pending orders from data-files stored at hdfs location /user/cloudera/problem3/parquet and save it into json file in HDFS

Output Requirement:
Result should be saved in /user/cloudera/problem3/orders_pending
Output file should be saved as json file.
Output file should Gzip compressed.

Important Information:
Please make sure you are running all your solutions on spark 1.6 since exam environment will be providing that.
*/
sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
  --password cloudera \
  --username root \
  --table orders \
  --as-parquetfile \
  --delete-target-dir \
  --target-dir /user/cloudera/problem3/parquet \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

val orders = sqlContext.read.parquet("/user/cloudera/problem3/parquet")
val pending = orders.filter("order_status like('%PENDING%')")
pending.toJSON.repartition(1).saveAsTextFile("/user/cloudera/problem3/orders_pending",classOf[org.apache.hadoop.io.compress.GzipCodec])

$ hdfs dfs -ls /user/cloudera/problem3/orders_pending
$ hdfs dfs -text /user/cloudera/problem3/orders_pending/part-00000.gz | tail -n 20