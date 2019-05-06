/*
Question 3:
Instructions:
Connect to mySQL database using sqoop, import all completed orders into HDFS directory.
Data Description:
A mysql instance is running on the gateway node.In that instance you will find customers table that contains customers data.
> Installation : on the cluser node gateway
> Database name: retail_db
> Table name: Orders
> Username: root
> Password: cloudera

Output Requirement:
Place the customers files in HDFS directory "/user/cloudera/problem1/orders/parquetdata"
Use parquet format with tab delimiter and snappy compression.
Null values are represented as -1 for numbers and "NA" for strings
*/
sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
  --username root \
  --password cloudera \
  --table orders \
  --where "order_status LIKE('COMPLETE')" \
  --fields-terminated-by '\t' \
  --null-string "NA" \
  --null-non-string -1 \
  --as-parquetfile \
  --compress \
--compression-codec snappy \
--delete-target-dir \
  --target-dir /user/cloudera/problem1/orders/parquetdata \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

$ hdfs dfs -ls /user/cloudera/problem1/orders/parquetdata
$ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/problem1/orders/parquetdata/5f94d536-3e3e-4fcc-97af-7b75e65579b5.parquet
$ parquet-tools head hdfs://quickstart.cloudera/user/cloudera/problem1/orders/parquetdata/5f94d536-3e3e-4fcc-97af-7b75e65579b5.parquet