/*
Question 1: Correct
Instructions:
Connect to mySQL database using sqoop, import all customers records into HDFS.
Data Description:
A mysql instance is running on the gateway node.In that instance you will find customers table that contains customers data.
> Installation : on the cluser node gateway
> Database name: retail_db
> Table name: Customers
> Username: root
> Password: cloudera
Output Requirement:
Place the customers files in HDFS directory "/user/cloudera/problem1/customers/text2"
Input fields should be separated with ^ and output should be compressed in deflate codec.
Only import customer_id, customer_fname, customer_lname, customer_street
*/
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table customers \
  --columns "customer_id,customer_fname,customer_lname,customer_street" \
  --as-textfile \
  --fields-terminated-by '^' \
  --compress \
--compression-codec deflate \
--delete-target-dir \
  --target-dir /user/cloudera/problem1/customers/text2 \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

$ hdfs dfs -ls /user/cloudera/problem1/customers/text2
$ hdfs dfs -text /user/cloudera/problem1/customers/text2/part-m-00000.deflate | head -n 50