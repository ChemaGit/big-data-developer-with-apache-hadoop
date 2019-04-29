/*
Question 3: Correct
PreRequiste:
Run below sqoop command to import customer table from mysql into hdfs to the destination /user/cloudera/problem3/all/customer/input as text file and fields seperated by tab character

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table customers \
--fields-terminated-by '\t' \
--target-dir /user/cloudera/problem3/all/customer/input

Instructions:
Get input from hdfs dir /user/cloudera/problem3/all/customer/input and save only first 4 field in the result as pipe delimited file in HDFS

Output Requirement:
Result should be saved in /user/cloudera/problem3/all/customer/output Output file should be saved in text format.
*/
sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
  --password cloudera \
  --username root \
  --table customers \
  --fields-terminated-by '\t' \
  --delete-target-dir \
  --target-dir /user/cloudera/problem3/all/customer/input \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

val customers = sc.textFile("/user/cloudera/problem3/all/customer/input").map(line => line.split('\t')).map(r => (r(0),r(1),r(2),r(3))).map(t => "%s|%s|%s|%s".format(t._1.toString,t._2,t._3,t._4))
customers.repartition(1).saveAsTextFile("/user/cloudera/problem3/all/customer/output")

$ hdfs dfs -ls /user/cloudera/problem3/all/customer/output
$ hdfs dfs -cat /user/cloudera/problem3/all/customer/output/p* | tail -n 50