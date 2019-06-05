/*
Question 7: Correct
PreRequiste:[Prerequisite section will not be there in actual exam]
Run below sqoop command

sqoop import --connect "jdbc:mysql://localhost/retail_db" --password cloudera --username root --table customers --columns "customer_id,customer_fname,customer_city" --target-dir /user/cloudera/problem8/customer-avro --as-avrodatafile

Instructions:
Create a metastore table from avro files provided at below location.
Input folder is /user/cloudera/problem8/customer-avro

Output Requirement:
Table name should be customer_parquet_avro
output location for hive data /user/cloudera/problem8/customer-parquet-hive
Output file should be saved in parquet format using GZIP compression.

[You will not be provided with any answer choice in actual exam.Below answers are just provided to guide you]
To check compression of generated parquet files, use parquet tools
parquet-tools meta hdfs://quickstart.cloudera:8020/user/cloudera/problem8/customer-parquet-hive/000000_0
avro-tools getschema hdfs://cloudera@quickstart:8020/user/cloudera/problem8/customer-avro/part-m-00000.avro > cust-avro.avsc
*/

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
  --password cloudera \
  --username root \
  --table customers \
  --columns "customer_id,customer_fname,customer_city" \
  --delete-target-dir \
  --target-dir /user/cloudera/problem8/customer-avro \
  --as-avrodatafile \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/outdir \
--num-mappers 1

import com.databricks.spark.avro._
val customers = sqlContext.read.avro("/user/cloudera/problem8/customer-avro")
customers.show(10)
sqlContext.setConf("spark.sql.parquet.compression.codec","gzip")
customers.repartition(1).write.parquet("/user/cloudera/problem8/customer-parquet-hive")
sqlContext.sql("""create table customer_parquet_avro(customer_id int,customer_fname string,customer_city string) STORED AS PARQUET LOCATION "/user/cloudera/problem8/customer-parquet-hive" TBLPROPERTIES("parquet.compression"="gzip")""")

$ hive
  hive> show tables;
hive> describe formatted customer_parquet_avro;
hive> select * from customer_parquet_avro limit 10;

$ hdfs dfs -ls /user/cloudera/problem8/customer-parquet-hive
$ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/problem8/customer-parquet-hive/part-r-00000-296e87b6-eca3-4942-9f7d-8366e3533906.gz.parquet
$ parquet-tools schema hdfs://quickstart.cloudera/user/cloudera/problem8/customer-parquet-hive/part-r-00000-296e87b6-eca3-4942-9f7d-8366e3533906.gz.parquet

/**SOLUTION WITH HIVE**/
CREATE EXTERNAL TABLE customer_avro_new(custId string,fname string,city string) STORED AS AVRO LOCATION '/user/cloudera/problem8/customer-avro' TBLPROPERTIES('avro.schema.url'='file:///home/cloudera/cust-avro.avsc');
CREATE TABLE customer_parquet_avro STORED AS PARQUET LOCATION '/user/clodera/problem8/customer-parquet-hive' TBLPROPERTIES('parquet.compression'='GZIP') AS SELECT * FROM customer_avro_new;