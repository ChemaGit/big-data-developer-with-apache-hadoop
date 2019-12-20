/*
Question 4: Correct
PreRequiste:
Run below sqoop command to import orders table from mysql into hdfs to the destination /user/cloudera/problem4_ques6/input as parquet file.

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table orders \
--target-dir /user/cloudera/problem4_ques6/input \
--as-parquetfile

Instructions:
Save the data to hdfs using no compression as sequence file.
Output Requirement:
Result should be saved in at /user/cloudera/problem4_ques6/output and fields should be seperated by pipe delimiter
*/
sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
  --password cloudera \
  --username root \
  --table orders \
  --delete-target-dir \
  --target-dir /user/cloudera/problem4_ques6/input \
  --as-parquetfile \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

val orders = sqlContext.read.parquet("/user/cloudera/problem4_ques6/input")
orders.rdd.map(r => (r(0).toString,r.mkString(","))).saveAsSequenceFile("/user/cloudera/problem4_ques6/output")

$ hdfs dfs -ls /user/cloudera/problem4_ques6/output
$ hdfs dfs -text /user/cloudera/problem4_ques6/output/part-00000 | head -n 20