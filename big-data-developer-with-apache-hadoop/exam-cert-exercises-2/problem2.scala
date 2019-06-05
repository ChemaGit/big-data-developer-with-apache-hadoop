/*
Question 2: Correct
PreRequiste:
[Prerequisite section will not be there in actual exam]
Run below sqoop command to import products table from mysql into hive table product_new:

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--username root \
--password cloudera \
--table products \
--hive-import \
--create-hive-table \
--hive-database default \
--hive-table product_replica \
-m 1

Instructions:
Get products from metastore table named "product_replica" whose price > 100 and save the results in HDFS in parquet format.
Output Requirement:
Result should be saved in /user/cloudera/practice1/problem8/product/output as parquet file
Files should be saved in Gzip compression.

[You will not be provided with any answer choice in actual exam.Below answers are just provided to guide you]
Important Information:

In case hivecontext does not get created in your environment or table not found issue occurs. Just check that SPARK_HOME/conf has hive_site.xml copied from /etc/hive/conf/hive_site.xml. If in case any derby lock issue occurs, delete SPARK_HOME/metastore_db/dbex.lck to release the lock.
*/
sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
  --username root \
  --password cloudera \
  --table products \
  --hive-import \
--create-hive-table \
  --hive-database default \
--hive-table product_replica \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
-m 1

sqlContext.setConf("spark.sql.parquet.compression.codec","gzip")
val result = sqlContext.sql("""select * from product_replica where product_price > 100""")
result.repartition(1).write.parquet("/user/cloudera/practice1/problem8/product/output")

$ hdfs dfs -ls /user/cloudera/practice1/problem8/product/output
$ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/practice1/problem8/product/output/part-r-00000-cf53ab19-c40b-4e9c-9c97-57831eaa6a55.gz.parquet
$ parquet-tools head hdfs://quickstart.cloudera/user/cloudera/practice1/problem8/product/output/part-r-00000-cf53ab19-c40b-4e9c-9c97-57831eaa6a55.gz.parquet