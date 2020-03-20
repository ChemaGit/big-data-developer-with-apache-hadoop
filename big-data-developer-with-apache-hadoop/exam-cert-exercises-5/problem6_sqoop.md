# Question 6:
````text
Instructions:
Connect to mySQL database using sqoop, import all orders whose id > 1000 into HDFS directory in bzip compression
Data Description:
A mysql instance is running on the gateway node.In that instance you will find orders table that contains orders data.
> Installation : on the cluser node gateway
> Database name: retail_db
> Table name: Orders
> Username: root
> Password: cloudera

Output Requirement:
Place the customers files in HDFS directory "/user/cloudera/problem1/orders_new/order_bzip"
Use avro format with tab delimiter and compressed with bzip compression
````

````bash
$ sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders \
--where "order_id > 1000" \
--fields-terminated-by '\t' \
--as-avrodatafile \
--compress \
--compression-codec org.apache.hadoop.io.compress.BZip2Codec \
--delete-target-dir \
--target-dir /user/cloudera/problem1/orders_new/order_bzip \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hdfs dfs -ls /user/cloudera/problem1/orders_new/order_bzip
$ avro-tools getmeta hdfs://quickstart.cloudera/user/cloudera/problem1/orders_new/order_bzip/part-m-00000.avro
$ avro-tools tojson hdfs://quickstart.cloudera/user/cloudera/problem1/orders_new/order_bzip/part-m-00000.avro | head -n 20
````

