/*
Question 6: Correct
Prerequisite:
[Prerequisite section will not be part of actual exam]
Import products table from mysql into hive metastore table named product_ranked in warehouse directory /user/cloudera/practice4.db. Run below sqoop statement

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--username root \
--password cloudera \
--table products \
--warehouse-dir /user/cloudera/practice4.db \
--hive-import \
--create-hive-table \
--hive-database default \
--hive-table product_ranked -m 1

Instructions:
Provided a meta-store table named product_ranked consisting of product details ,find the most expensive product in each category.

Output Requirement:
Output should have product_category_id ,product_name,product_price,rank.
Result should be saved in /user/cloudera/pratice4/output/ as pipe delimited text file
*/
sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
  --username root \
  --password cloudera \
  --table products \
  --warehouse-dir /user/cloudera/practice4.db \
  --hive-import \
--create-hive-table \
  --hive-database default \
--hive-table product_ranked \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
-m 8

$ hive
  hive> show tables;
hive> describe product_ranked;
hive> select * from product_ranked limit 10;

product_id          	int
  product_category_id 	int
  product_name        	string
  product_description 	string
  product_price       	double
  product_image       	string

val rank = sqlContext.sql("""SELECT product_category_id, product_name,product_price, rank() over(partition by product_category_id order by product_price desc) as rank FROM product_ranked order by product_category_id, rank""")
val result = rank.filter("rank = 1")
result.rdd.map(r => r.mkString("|")).saveAsTextFile("/user/cloudera/pratice4/output/")

$ hdfs dfs -ls /user/cloudera/pratice4/output/
  $ hdfs dfs -cat /user/cloudera/pratice4/output/part-00000 | head -n 50