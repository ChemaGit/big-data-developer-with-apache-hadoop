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

//1. Import orders table from mysql as text file to the destination /user/cloudera/problem5/text. Fields should be terminated by a tab character ("\t") character and lines should be terminated by new line character ("\n"). 
sqoop import \
--connect jdbc:mysql://localhost/retail_db \
--username cloudera \
--password cloudera \
--table orders \
--delete-target-dir \
--target-dir /user/cloudera/problem5/text \
--fields-terminated-by '\t' \
--lines-terminated-by '\n' \
--num-mappers 1

//2. Import orders table from mysql  into hdfs to the destination /user/cloudera/problem5/avro. File should be stored as avro file.
sqoop import \
--connect jdbc:mysql://localhost/retail_db \
--username cloudera \
--password cloudera \
--table orders \
--delete-target-dir \
--target-dir /user/cloudera/problem5/avro \
--as-avrodatafile \
--num-mappers 1

//3. Import orders table from mysql  into hdfs  to folders /user/cloudera/problem5/parquet. File should be stored as parquet file.
sqoop import \
--connect jdbc:mysql://localhost/retail_db \
--username cloudera \
--password cloudera \
--table orders \
--delete-target-dir \
--target-dir /user/cloudera/problem5/parquet \
--as-parquetfile \
--num-mappers 1

//4. Transform/Convert data-files at /user/cloudera/problem5/avro and store the converted file at the following locations and file formats
val dataFile = sqlContext.read.avro("/user/cloudera/problem5/avro");

	//-save the data to hdfs using snappy compression as parquet file at /user/cloudera/problem5/parquet-snappy-compress
		sqlContext.setConf("spark.sql.parquet.compression.codec","snappy");
		dataFile.repartition(1).write.parquet("/user/cloudera/problem5/parquet-snappy-compress");
	//-save the data to hdfs using gzip compression as text file at /user/cloudera/problem5/text-gzip-compress
		dataFile.map(x=> x(0)+"\t"+x(1)+"\t"+x(2)+"\t"+x(3)).saveAsTextFile("/user/cloudera/problem5/text-gzip-compress",classOf[org.apache.hadoop.io.compress.GzipCodec]);
	//-save the data to hdfs using no compression as sequence file at /user/cloudera/problem5/sequence
		dataFile.map(x=> (x(0).toString,x(0)+"\t"+x(1)+"\t"+x(2)+"\t"+x(3))).saveAsSequenceFile("/user/cloudera/problem5/sequence");
	//-save the data to hdfs using snappy compression as text file at /user/cloudera/problem5/text-snappy-compress
		//Below may fail in some cloudera VMS. If the spark command fails use the sqoop command to accomplish the problem. Remember you need to get out to spark shell to run the sqoop command. 
		dataFile.map(x=> x(0)+"\t"+x(1)+"\t"+x(2)+"\t"+x(3)).saveAsTextFile("/user/cloudera/problem5/text-snappy-compress",classOf[org.apache.hadoop.io.compress.SnappyCodec]);
		//We can use Sqoop too to accomplish this task
		sqoop import \
		--table orders \ 
		--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
		--username retail_dba \
		--password cloudera \
		--as-textfile \
		-- num-mappers 1 \
		--target-dir user/cloudera/problem5/text-snappy-compress \
		--compress \
		--compression-codec org.apache.hadoop.io.compress.SnappyCodec

//5. Transform/Convert data-files at /user/cloudera/problem5/parquet-snappy-compress and store the converted file at the following locations and file formats
val parquetDataFile = sqlContext.read.parquet("/user/cloudera/problem5/parquet-snappy-compress");
	//-save the data to hdfs using no compression as parquet file at /user/cloudera/problem5/parquet-no-compress
	sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed");
	parquetDataFile.repartition(1).write.parquet("/user/cloudera/problem5/parquet-no-compress");
	//-save the data to hdfs using snappy compression as avro file at /user/cloudera/problem5/avro-snappy
	sqlContext.setConf("spark.sql.avro.compression.codec","snappy");
	parquetDataFile.write.avro("/user/cloudera/problem5/avro-snappy");

//6. Transform/Convert data-files at /user/cloudera/problem5/avro-snappy and store the converted file at the following locations and file formats
val avroData = sqlContext.read.avro("/user/cloudera/problem5/avro-snappy")
	//-save the data to hdfs using no compression as json file at /user/cloudera/problem5/json-no-compress
	avroData.toJSON.saveAsTextFile("/user/cloudera/problem5/json-no-compress");
	//-save the data to hdfs using gzip compression as json file at /user/cloudera/problem5/json-gzip
	avroData.toJSON.saveAsTextFile("/user/cloudera/problem5/json-gzip",classOf[org.apache.hadoop.io.GzipCodec]);

//7. Transform/Convert data-files at  /user/cloudera/problem5/json-gzip and store the converted file at the following locations and file formats
val jsonData = sqlContext.read.json("/user/cloudera/problem5/json-gzip")
	//-save the data to as comma separated text using gzip compression at   /user/cloudera/problem5/csv-gzip
	jsonData.map(x=>x(0)+","+x(1)+","+x(2)+","+x(3)).saveAsTextFile("/user/cloudera/problem5/csv-gzip",classOf[org.apache.hadoop.io.compress.GzipCodec])

//8. Using spark access data at /user/cloudera/problem5/sequence and stored it back to hdfs using no compression as ORC file to HDFS to destination /user/cloudera/problem5/orc 
//To read the sequence file you need to understand the sequence getter for the key and value class to 
//be used while loading the sequence file as a spark RDD.
//In a new terminal Get the Sequence file to local file system
$ hadoop fs -get /user/cloudera/problem5/sequence/part-00000
//read the first 300 characters to understand the two classes to be used. 
$ cut -c-300 part-00000

//In spark shell do below
val seqData = sc.sequenceFile("/user/cloudera/problem5/sequence/",classOf[org.apache.hadoop.io.Text],classOf[org.apache.hadoop.io.Text]);
seqData.map(x=>{var d = x._2.toString.split("\t"); (d(0),d(1),d(2),d(3))}).toDF().write.orc("/user/cloudera/problem5/orc");