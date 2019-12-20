/*
Question 3: Correct
PreRequiste:
[Prerequisite section will not be there in actual exam]

Run below sqoop command to import orders table from mysql into hdfs to the destination /user/cloudera/problem2/avro as avro file.
sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table orders \
--as-avrodatafile \
--target-dir /user/cloudera/problem2/avro

Instructions:

Convert data-files stored at hdfs location /user/cloudera/problem2/avro into parquet file using snappy compression and save in HDFS.

Output Requirement:

Result should be saved in /user/cloudera/problem2/parquet-snappy
Output file should be saved as Parquet file in Snappy Compression.
*/
sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
  --password cloudera \
  --username root \
  --table orders \
  --as-avrodatafile \
  --delete-target-dir \
  --target-dir /user/cloudera/problem2/avro \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

import org.apache.avro._
import com.databricks.spark.avro._

val orders = sqlContext.read.avro("/user/cloudera/problem2/avro")
orders.show(10)
sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
orders.write.parquet("/user/cloudera/problem2/parquet-snappy")

$ hdfs dfs -ls /user/cloudera/problem2/parquet-snappy
$ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/problem2/parquet-snappy/part-r-00000-c98c1300-857d-40ea-bda4-92b24b7ea937.snappy.parquet
$ parquet-tools head hdfs://quickstart.cloudera/user/cloudera/problem2/parquet-snappy/part-r-00000-c98c1300-857d-40ea-bda4-92b24b7ea937.snappy.parquet

// Explanation
// To run in local cloudera vm, open spark shell using "spark-shell --packages com.databricks:spark-avro_2.11:4.0.0". In exam ,just use spark-shell.
