/*
Question 6: Correct
PreRequiste:
[PreRequiste will not be there in actual exam]
Run below sqoop command to import customers table from mysql into hive table customers_hive:

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--username root \
--password cloudera \
--table customers \
--warehouse-dir /user/cloudera/problem3/customers_hive/input \
--hive-import \
--create-hive-table \
--hive-database default \
--hive-table customers_hive

Instructions:
Get Customers from metastore table named "customers_hive" whose fname is like "Rich" and save the results in HDFS in text format.

Output Requirement:
Result should be saved in /user/cloudera/practice2/problem4/customers/output as text file. Output should contain only fname, lname and city
fname and lname should seperated by tab with city seperated by colon

Sample Output
Richard Plaza:Francisco
Rich Smith:Chicago

*/
sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
  --username root \
  --password cloudera \
  --table customers \
  --warehouse-dir /user/cloudera/problem3/customers_hive/input \
  --hive-import \
--create-hive-table \
  --hive-database default \
--hive-table customers_hive \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

$ hive
  hive> use default;
hive> show tables;
hive> select * from customers_hive limit 10;
hive> describe customers_hive;
hive> exit;

sqlContext.sql("use default")
val result = sqlContext.sql("""SELECT CONCAT(customer_fname,"\t",customer_lname,":",customer_city) as result FROM customers_hive WHERE customer_fname LIKE("%Rich%")""")
result.rdd.map(r => r.mkString("")).repartition(1).saveAsTextFile("/user/cloudera/practice2/problem4/customers/output")

$ hdfs dfs -ls /user/cloudera/practice2/problem4/customers/output
$ hdfs dfs -cat /user/cloudera/practice2/problem4/customers/output/par* | head -n 20