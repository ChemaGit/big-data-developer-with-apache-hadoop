/**
Problem 4:
1. Import orders table from mysql as text file to the destination /user/cloudera/problem5/text. Fields should be terminated by a tab character ("\t") character and lines should be terminated by new line character ("\n"). 
2. Import orders table from mysql  into hdfs to the destination /user/cloudera/problem5/avro. File should be stored as avro file.
3. Import orders table from mysql  into hdfs  to folders /user/cloudera/problem5/parquet. File should be stored as parquet file.
4. Transform/Convert data-files at /user/cloudera/problem5/avro and store the converted file at the following locations and file formats
	-save the data to hdfs using snappy compression as parquet file at /user/cloudera/problem5/parquet-snappy-compress
	-save the data to hdfs using gzip compression as text file at /user/cloudera/problem5/text-gzip-compress
	-save the data to hdfs using no compression as sequence file at /user/cloudera/problem5/sequence
	-save the data to hdfs using snappy compression as text file at /user/cloudera/problem5/text-snappy-compress
5. Transform/Convert data-files at /user/cloudera/problem5/parquet-snappy-compress and store the converted file at the following locations and file formats
	-save the data to hdfs using no compression as parquet file at /user/cloudera/problem5/parquet-no-compress
	-save the data to hdfs using snappy compression as avro file at /user/cloudera/problem5/avro-snappy
6. Transform/Convert data-files at /user/cloudera/problem5/avro-snappy and store the converted file at the following locations and file formats
	-save the data to hdfs using no compression as json file at /user/cloudera/problem5/json-no-compress
	-save the data to hdfs using gzip compression as json file at /user/cloudera/problem5/json-gzip
7. Transform/Convert data-files at  /user/cloudera/problem5/json-gzip and store the converted file at the following locations and file formats
	-save the data to as comma separated text using gzip compression at   /user/cloudera/problem5/csv-gzip
8. Using spark access data at /user/cloudera/problem5/sequence and stored it back to hdfs using no compression as ORC file to HDFS to destination /user/cloudera/problem5/orc 
	*/

// 1. Import orders table from mysql as text file to the destination /user/cloudera/problem5/text. Fields should be terminated by a tab character ("\t") character and lines should be terminated by new line character ("\n").
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
	--username root \
	--password cloudera \
	--table orders \
	--as-textfile \
	--fields-terminated-by '\t' \
	--lines-terminated-by '\n' \
	--delete-target-dir \
	--target-dir /user/cloudera/problem5/text \
	--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

$ hdfs dfs -ls /user/cloudera/problem5/text
$ hdfs dfs -tail /user/cloudera/problem5/text/part-m-00000

// 2. Import orders table from mysql  into hdfs to the destination /user/cloudera/problem5/avro. File should be stored as avro file.
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
	--username root \
	--password cloudera \
	--table orders \
	--as-avrodatafile \
	--delete-target-dir \
	--target-dir /user/cloudera/problem5/avro \
	--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

$ hdfs dfs -ls /user/cloudera/problem5/avro
$ avro-tools tojson hdfs://quickstart.cloudera/user/cloudera/problem5/avro/part-m-00000.avro | head -n 10

// 3. Import orders table from mysql  into hdfs  to folders /user/cloudera/problem5/parquet. File should be stored as parquet file.
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
	--username root \
	--password cloudera \
	--table orders \
	--as-parquetfile \
	--delete-target-dir \
	--target-dir /user/cloudera/problem5/parquet \
	--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

$ hdfs dfs -ls /user/cloudera/problem5/parquet
$ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/problem5/parquet/3538ab90-0e57-4e4d-89db-706d397c1f7c.parquet | head -n 10

// 4. Transform/Convert data-files at /user/cloudera/problem5/avro and store the converted file at the following locations and file formats
import com.databricks.spark.avro._
val orders = sqlContext.read.avro("/user/cloudera/problem5/avro")
//	-save the data to hdfs using snappy compression as parquet file at /user/cloudera/problem5/parquet-snappy-compress
sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
orders.write.parquet("/user/cloudera/problem5/parquet-snappy-compress")
//	-save the data to hdfs using gzip compression as text file at /user/cloudera/problem5/text-gzip-compress
orders.repartition(1).rdd.map(r => r.mkString(",")).saveAsTextFile("/user/cloudera/problem5/text-gzip-compress",classOf[org.apache.hadoop.io.compress.GzipCodec])
//	-save the data to hdfs using no compression as sequence file at /user/cloudera/problem5/sequence
orders.repartition(1).rdd.map(r => (r(0).toString,r.mkString(","))).saveAsSequenceFile("/user/cloudera/problem5/sequence")
//	-save the data to hdfs using snappy compression as text file at /user/cloudera/problem5/text-snappy-compress
orders.repartition(1).rdd.map(r => r.mkString(",")).saveAsTextFile("/user/cloudera/problem5/text-snappy-compress",classOf[org.apache.hadoop.io.compress.SnappyCodec])
// if the line above fails you may try the same with sqoop
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
	--username root \
	--password cloudera \
	--table orders \
	--compress \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
	--as-textfile \
	--delete-target-dir \
	--target-dir /user/cloudera/problem5/text-snappy-compress \
	--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

// check the outputs
$ hdfs dfs -ls /user/cloudera/problem5/parquet-snappy-compress
$ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/problem5/parquet-snappy-compress/part-r-00000-f6d9a67d-fdb1-404c-8336-97f404118b78.snappy.parquet
$ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/problem5/parquet-snappy-compress/part-r-00000-f6d9a67d-fdb1-404c-8336-97f404118b78.snappy.parquet | head -n 20

$ hdfs dfs -ls /user/cloudera/problem5/text-gzip-compress
$ hdfs dfs -text /user/cloudera/problem5/text-gzip-compress/part-00000.gz | head -n 20

$ hdfs dfs -ls /user/cloudera/problem5/sequence
$ hdfs dfs -text /user/cloudera/problem5/sequence/part-00000 | head -n 10

$ hdfs dfs -ls /user/cloudera/problem5/text-snappy-compress
$ hdfs dfs -text /user/cloudera/problem5/text-snappy-compress/part-m-00000.snappy | tail -n 10

// 5. Transform/Convert data-files at /user/cloudera/problem5/parquet-snappy-compress and store the converted file at the following locations and file formats
val orders = sqlContext.read.parquet("/user/cloudera/problem5/parquet-snappy-compress")
//	-save the data to hdfs using no compression as parquet file at /user/cloudera/problem5/parquet-no-compress
sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")
orders.repartition(1).write.parquet("/user/cloudera/problem5/parquet-no-compress")
//	-save the data to hdfs using snappy compression as avro file at /user/cloudera/problem5/avro-snappy
import com.databricks.spark.avro._
sqlContext.setConf("spark.sql.avro.compression.codec","snappy")
orders.repartition(1).write.avro("/user/cloudera/problem5/avro-snappy")
// check the outputs
$ hdfs dfs -ls /user/cloudera/problem5/parquet-no-compress
$ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/problem5/parquet-no-compress/part-r-00000-e9b32bb9-47be-4994-819e-4b350276ba99.parquet
$ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/problem5/parquet-no-compress/part-r-00000-e9b32bb9-47be-4994-819e-4b350276ba99.parquet | tail -n 20

$ hdfs dfs -ls /user/cloudera/problem5/avro-snappy
$ avro-tools getmeta hdfs://quickstart.cloudera/user/cloudera/problem5/avro-snappy/part-r-00000-654f8dd3-c0e5-4fb0-94af-7faacf2d3c0b.avro
$ avro-tools tojson hdfs://quickstart.cloudera/user/cloudera/problem5/avro-snappy/part-r-00000-654f8dd3-c0e5-4fb0-94af-7faacf2d3c0b.avro | head -n 20

// 6. Transform/Convert data-files at /user/cloudera/problem5/avro-snappy and store the converted file at the following locations and file formats
import com.databricks.spark.avro._
val orders = sqlContext.read.avro("/user/cloudera/problem5/avro-snappy")
//	-save the data to hdfs using no compression as json file at /user/cloudera/problem5/json-no-compress
orders.repartition(1).toJSON.saveAsTextFile("/user/cloudera/problem5/json-no-compress")
//	-save the data to hdfs using gzip compression as json file at /user/cloudera/problem5/json-gzip
orders.repartition(1).toJSON.saveAsTextFile("/user/cloudera/problem5/json-gzip",classOf[org.apache.hadoop.io.compress.GzipCodec])
// check the outputs
$ hdfs dfs -ls /user/cloudera/problem5/json-no-compress
$ hdfs dfs -text /user/cloudera/problem5/json-no-compress/part-00000 | tail -n 20

$ hdfs dfs -ls /user/cloudera/problem5/json-gzip
$ hdfs dfs -text /user/cloudera/problem5/json-gzip/part-00000.gz | tail -n 20

// 7. Transform/Convert data-files at  /user/cloudera/problem5/json-gzip and store the converted file at the following locations and file formats
val orders = sqlContext.read.json("/user/cloudera/problem5/json-gzip")
//	-save the data to as comma separated text using gzip compression at   /user/cloudera/problem5/csv-gzip
orders.repartition(1).rdd.map(r => r.mkString(",")).saveAsTextFile("/user/cloudera/problem5/csv-gzip", classOf[org.apache.hadoop.io.compress.GzipCodec])

$ hdfs dfs -ls /user/cloudera/problem5/csv-gzip
$ hdfs dfs -text /user/cloudera/problem5/csv-gzip/part-00000.gz | tail -n 20

// 8. Using spark access data at /user/cloudera/problem5/sequence and stored it back to hdfs using no compression as ORC file to HDFS to destination /user/cloudera/problem5/orc
val ordersSequence = sc.sequenceFile("/user/cloudera/problem5/sequence",classOf[org.apache.hadoop.io.Text],classOf[org.apache.hadoop.io.Text])
val orders = ordersSequence.map(t => t._2.toString).map(line => line.split(",")).map(r => (r(0).toInt,r(1).toLong,r(2).toInt,r(3).toString)).toDF
orders.repartition(1).write.orc("/user/cloudera/problem5/orc")

$ hdfs dfs -ls /user/cloudera/problem5/orc
$ hdfs dfs -text /user/cloudera/problem5/orc/part-r-00000-9026d0fd-6074-413e-ad56-c4e6f7776c4b.orc