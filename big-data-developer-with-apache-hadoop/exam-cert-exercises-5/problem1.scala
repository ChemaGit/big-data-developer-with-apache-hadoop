/*
Question 1:
Instructions:
Connect to mySQL database using sqoop, import all orders whose id > 1000 into HDFS directory in gzip codec
Data Description:
A mysql instance is running on the gateway node.In that instance you will find customers table that contains customers data.
> Installation : on the cluser node gateway
> Database name: retail_db
> Table name: Orders
> Username: root
> Password: cloudera

Output Requirement:
Place the customers files in HDFS directory "/user/cloudera/problem1/orders_new/parquetdata"
Use parquet format with tab delimiter and compressed with gzip codec
*/
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table orders \
  --where "order_id > 1000" \
  --as-parquetfile \
  --delete-target-dir \
  --target-dir /user/cloudera/problem1/orders_new/parquet \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

val orders = sqlContext.read.parquet("/user/cloudera/problem1/orders_new/parquet")
orders.show()
sqlContext.setConf("spark.sql.parquet.compression.codec","gzip")
orders.write.parquet("/user/cloudera/problem1/orders_new/parquetdata")

$ hdfs dfs -ls /user/cloudera/problem1/orders_new/parquetdata
$ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/problem1/orders_new/parquetdata/part-r-00000-d5c9e54b-4b9d-4653-aa75-914b34aca8ae.gz.parquet
$ parquet-tools head hdfs://quickstart.cloudera/user/cloudera/problem1/orders_new/parquetdata/part-r-00000-d5c9e54b-4b9d-4653-aa75-914b34aca8ae.gz.parquet