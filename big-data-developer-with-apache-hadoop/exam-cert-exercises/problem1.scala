/*Question 1: Correct
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
Place the customers files in HDFS directory "/user/cloudera/problem1/customers/avrodata"
Use avro format with pipe delimiter and snappy compression.
Load every customer record completely
*/

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table customers \
  --where "customer_state = 'CA'" \
  --fields-terminated-by '|' \
  --as-avrodatafile \
  --compress \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
  --delete-target-dir \
  --target-dir /user/cloudera/problem1/customers/avrodata \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

// 5. Read avro file data and metadata using avro tools: avro-tools tojson | getmeta part*00*.avro

$ avro-tools getmeta hdfs://quickstart.cloudera/user/cloudera/problem1/customers/avrodata/part-m-00000.avro
$ avro-tools tojson hdfs://quickstart.cloudera/user/cloudera/problem1/customers/avrodata/part-m-00000.avro