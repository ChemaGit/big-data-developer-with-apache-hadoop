/*
Question 7: Correct
PreRequiste:
[Prerequisite section will not be there in actual exam]
Run below sqoop command to import few columns from customer table from mysql into hdfs to the destination /user/cloudera/practice1/problem7/customer/avro_snappy as avro file.

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table customers \
--target-dir /user/cloudera/practice1/problem7/customer/avro \
--columns "customer_id,customer_fname,customer_lname" --as-avrodatafile

Instructions:
Convert data-files stored at hdfs location /user/cloudera/practice1/problem7/customer/avro into tab delimited file using gzip compression and save in HDFS.

Output Requirement:
Result should be saved in /user/cloudera/practice1/problem7/customer_text_gzip Output file should be saved as tab delimited file in gzip Compression.

Sample Output:

21 Andrew Smith
111 Mary Jons
*/
sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
  --password cloudera \
  --username root \
  --table customers \
  --delete-target-dir \
  --target-dir /user/cloudera/practice1/problem7/customer/avro \
  --columns "customer_id,customer_fname,customer_lname" \
  --as-avrodatafile \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

import com.databricks.spark.avro._
val customers = sqlContext.read.avro("/user/cloudera/practice1/problem7/customer/avro")
customers.show()
customers.rdd.map(r => r.mkString("\t")).saveAsTextFile("/user/cloudera/practice1/problem7/customer_text_gzip", classOf[org.apache.hadoop.io.compress.GzipCodec])

$ hdfs dfs -ls /user/cloudera/practice1/problem7/customer_text_gzip
$ hdfs dfs -text /user/cloudera/practice1/problem7/customer_text_gzip/part-00000.gz