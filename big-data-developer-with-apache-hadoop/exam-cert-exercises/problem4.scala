/*
Question 4: Correct
Prerequiste:
[Prerequisite section will not be there in actual exam]

Import orders table from mysql into hdfs location /user/cloudera/practice4/question3/orders/.Run below sqoop statement

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--username root \
--password cloudera \
--table orders \
--target-dir /user/cloudera/practice4/question3/orders/

Import customers from mysql into hdfs location /user/cloudera/practice4/question3/customers/.Run below sqoop statement

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--username root \
--password cloudera \
--table customers \
--target-dir /user/cloudera/practice4/question3/customers/ \
--columns "customer_id,customer_fname,customer_lname"

Instructions:

Join the data at hdfs location /user/cloudera/practice4/question3/orders/ & /user/cloudera/practice4/question3/customers/ to find out customers whose orders status is like "pending"
Schema for customer File
Customer_id,customer_fname,customer_lname
Schema for Order File
Order_id,order_date,order_customer_id,order_status

Output Requirement:
Output should have customer_id,customer_fname,order_id and order_status.Result should be saved in /user/cloudera/p1/q7/output
*/
sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
  --username root \
  --password cloudera \
  --table orders \
  --delete-target-dir \
  --target-dir /user/cloudera/practice4/question3/orders/ \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
  --username root \
  --password cloudera \
  --table customers \
  --delete-target-dir \
  --target-dir /user/cloudera/practice4/question3/customers/ \
--columns "customer_id,customer_fname,customer_lname" \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

val orders = sc.textFile("/user/cloudera/practice4/question3/orders/").map(line => line.split(",")).map(arr => (arr(0).toInt,arr(1),arr(2).toInt,arr(3))).toDF("order_id","order_date","order_customer_id","order_status")
val customer = sc.textFile("/user/cloudera/practice4/question3/customers/").map(line => line.split(",")).map(arr => (arr(0),arr(1),arr(2))).toDF("customer_id","customer_fname","customer_lname")

orders.registerTempTable("o")
customer.registerTempTable("c")

sqlContext.sql("""select customer_id,customer_fname,order_id,order_status from c join o on(c.customer_id = o.order_customer_id) where order_status like("%PENDING%")""").show(10)
val result = sqlContext.sql("""select customer_id,customer_fname,order_id,order_status from c join o on(c.customer_id = o.order_customer_id) where order_status like("%PENDING%")""")

result.rdd.map(r => r.mkString(",")).saveAsTextFile("/user/cloudera/p1/q7/output")

$ hdfs dfs -ls /user/cloudera/p1/q7/output
$ hdfs dfs -cat /user/cloudera/p1/q7/output/part-00000 | head -n 20