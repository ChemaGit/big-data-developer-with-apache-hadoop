/*
Question 2: Correct
Prerequistee:
Import products table from mysql into hive metastore table named product_ranked_new in warehouse directory /user/cloudera/practice5.db. Run below sqoop statement

sqoop import \
--connect "jdbc:mysql://gateway/retail_db" \
--username root \
--password cloudera \
--table products \
--warehouse-dir /user/cloudera/practice5.db \
--hive-import \
--create-hive-table \
--hive-database default \
--hive-table product_ranked_new \
-m 1

Instructions:
using product_ranked_new metastore table, Find the most expensive products within each category
Output Requirement:
Output should have product_id,product_name,product_price,product_category_id.Result should be saved in /user/cloudera/pratice4/question2/output
*/
sqoop import \
--connect "jdbc:mysql://quickstart:3306/retail_db" \
  --username root \
  --password cloudera \
  --table products \
  --warehouse-dir /user/cloudera/practice5.db \
  --hive-import \
--create-hive-table \
  --hive-database default \
--hive-table product_ranked_new \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
-m 1

sqlContext.sql("use default")
val ranked = sqlContext.sql("""SELECT product_id,product_name,product_price,product_category_id, rank() over(partition by product_category_id order by product_price desc) as rank FROM product_ranked_new order by product_category_id desc, rank""")
ranked.registerTempTable("ranked")
val result = sqlContext.sql("""SELECT product_id,product_name,product_price,product_category_id FROM ranked WHERE rank = 1 order by product_category_id""")
result.rdd.map(r => r.mkString(",")).repartition(1).saveAsTextFile("/user/cloudera/pratice4/question2/output")

$ hdfs dfs -ls /user/cloudera/pratice4/question2/output
$ hdfs dfs -cat /user/cloudera/pratice4/question2/output/part* | head -n 50