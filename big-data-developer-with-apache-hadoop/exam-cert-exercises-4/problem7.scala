/*
Question 7: Correct
PreRequiste:
Import order_items and products into HDFS from mysql

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--username root \
--password cloudera \
--table order_items \
--target-dir /user/cloudera/practice4_ques6/order_items/

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--username root \
--password cloudera \
--table products \
--target-dir /user/cloudera/practice4_ques6/products/ \
-m 1

Instructions:
Find top 10 products which has made highest revenue. Products and order_items data are placed in HDFS directory /user/cloudera/practice4_ques6/order_items/ and /user/cloudera/practice4_ques6/products/ respectively.

Given Below case classes to be used:
case class Products(pId:Integer,name:String)
case class Orders(prodId:Integer,order_total:Float)

Data Description:
A mysql instance is running on the gateway node.In that instance you will find products table.
> Installation : on the cluser node gateway
> Database name: retail_db
> Table name: Products, Order_items
> Username: root
> Password: cloudera
Output Requirement:
Output should have product_id and revenue seperated with ':' and should be saved in /user/cloudera/practice4_ques6/output
*/
sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
  --username root \
  --password cloudera \
  --table order_items \
  --delete-target-dir \
  --target-dir /user/cloudera/practice4_ques6/order_items/ \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
  --username root \
  --password cloudera \
  --table products \
  --delete-target-dir \
  --target-dir /user/cloudera/practice4_ques6/products/ \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

case class Products(pId:Integer,name:String)
case class Orders(prodId:Integer,order_total:Float)

val orderItems = sc.textFile("/user/cloudera/practice4_ques6/order_items/").map(line => line.split(",")).map(r => Orders(r(2).toInt,r(4).toFloat)).toDF
val products = sc.textFile("/user/cloudera/practice4_ques6/products/").map(line => line.split(",")).map(r => Products(r(0).toInt,r(2))).toDF

orderItems.registerTempTable("oi")
products.registerTempTable("p")
val result = sqlContext.sql("""SELECT pId, ROUND(SUM(order_total), 2) as total_revenue FROM p JOIN oi ON(p.pId = oi.prodId) GROUP BY pId ORDER BY total_revenue DESC LIMIT 10""")
result.rdd.map(r => r.mkString(":")).saveAsTextFile("/user/cloudera/practice4_ques6/output")

$ hdfs dfs -ls /user/cloudera/practice4_ques6/output
$ hdfs dfs -cat /user/cloudera/practice4_ques6/output/p*