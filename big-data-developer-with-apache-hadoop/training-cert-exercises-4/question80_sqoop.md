/** Question 80
  * Problem Scenario 6 : You have been given following mysql database details as well as other info.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Compression Codec : org.apache.hadoop.io.compress.SnappyCodec
  * Please accomplish following.
  * 1. Import entire database such that it can be used as a hive tables, it must be created in default schema.
  * 2. Also make sure each tables file is partitioned in 3 files e.g. part-00000, part-00002, part-
  * 3. Store all the Java files in a directory called outdir/bindir to evalute the further
  */
sqoop import-all-tables \
  --connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --hive-import \
--hive-database default \
--create-hive-table \
  --hive-overwrite \
  --compress \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--autoreset-to-one-mapper

$ hdfs dfs -ls /user/hive/warehouse
$ hdfs dfs -ls /user/hive/warehouse/departments

$ hive
  hive> use default;
hive> show tables:
  hive> select * from departments;