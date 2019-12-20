/*
Question 5:
Instructions:
Connect to mySQL database using sqoop, import all customers that lives in 'CA' state.
Data Description:
A mysql instance is running on the gateway node.In that instance you will find customers table that contains customers data.
> Installation : on the cluser node gateway
> Database name: retail_db
> Table name: Customers
> Username: root
> Password: cloudera

Output Requirement:
Place the customers files in HDFS directory "/user/cloudera/problem1/customers_selected/avrodata"
Use avro format with pipe delimiter and snappy compression.
Load every only customer_id,customer_fname,customer_lname
*/
sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
  --username root \
  --password cloudera \
  --table customers \
  --columns "customer_id,customer_fname,customer_lname" \
  --where "customer_state LIKE('CA')" \
  --fields-terminated-by '|' \
  --as-avrodatafile \
  --compress \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
  --delete-target-dir \
  --target-dir /user/cloudera/problem1/customers_selected/avrodata \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hdfs dfs -ls /user/cloudera/problem1/customers_selected/avrodata
$ avro-tools getmeta hdfs://quickstart.cloudera/user/cloudera/problem1/customers_selected/avrodata/part-m-00000.avro
$ avro-tools tojson hdfs://quickstart.cloudera/user/cloudera/problem1/customers_selected/avrodata/part-m-00000.avro | head -n 20