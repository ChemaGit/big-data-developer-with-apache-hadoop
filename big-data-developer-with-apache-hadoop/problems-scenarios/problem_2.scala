/**
Problem 2:
1. Using sqoop copy data available in mysql products table to folder /user/cloudera/products on hdfs as text file. columns should be delimited by pipe '|'
2. move all the files from /user/cloudera/products folder to /user/cloudera/problem2/products folder
3. Change permissions of all the files under /user/cloudera/problem2/products such that owner has read,write and execute permissions, group has read and write permissions whereas others have just read and execute permissions
4. read data in /user/cloudera/problem2/products and do the following operations using a) dataframes api b) spark sql c) RDDs aggregateByKey method. 
   Your solution should have three sets of steps. 
   Sort the resultant dataset by category id
	- filter such that your RDD\DF has products whose price is lesser than 100 USD
	- on the filtered data set find out the higest value in the product_price column under each category
	- on the filtered data set also find out total products under each category
	- on the filtered data set also find out the average price of the product under each category
	- on the filtered data set also find out the minimum price of the product under each category
5. store the result in avro file using snappy compression under these folders respectively
	- /user/cloudera/problem2/products/result-df
	- /user/cloudera/problem2/products/result-sql
	- /user/cloudera/problem2/products/result-rdd
	*/

// 1. Using sqoop copy data available in mysql products table to folder /user/cloudera/products on hdfs as text file. columns should be delimited by pipe '|'
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
	--username root \
	--password cloudera \
	--table products \
	--fields-terminated-by "|" \
	--as-textfile \
	--delete-target-dir \
	--target-dir /user/cloudera/products \
	--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hdfs dfs -ls /user/cloudera/products
$ hdfs dfs -cat /user/cloudera/products/part-m-00000 | head -n 10

// 2. move all the files from /user/cloudera/products folder to /user/cloudera/problem2/products folder
$ hdfs dfs -mkdir /user/cloudera/problem2
$ hdfs dfs -mkdir /user/cloudera/problem2/products
$ hdfs dfs -mv /user/cloudera/products/* /user/cloudera/problem2/products
$ hdfs dfs -ls /user/cloudera/problem2/products

// 3. Change permissions of all the files under /user/cloudera/problem2/products such that owner has read,write and execute permissions, group has read and write permissions whereas others have just read and execute permissions
$ hdfs dfs -ls /user/cloudera/problem2/products
$ hdfs dfs -chmod 765 /user/cloudera/problem2/products/*
$ hdfs dfs -ls /user/cloudera/problem2/products

// 4. read data in /user/cloudera/problem2/products and do the following operations using a) dataframes api b) spark sql c) RDDs aggregateByKey method.
//   Your solution should have three sets of steps.
//   Sort the resultant dataset by category id
//	- filter such that your RDD\DF has products whose price is lesser than 100 USD
//	- on the filtered data set find out the higest value in the product_price column under each category
//	- on the filtered data set also find out total products under each category
//	- on the filtered data set also find out the average price of the product under each category
//	- on the filtered data set also find out the minimum price of the product under each category
val filt = List("", " ")
val products = sc.textFile("/user/cloudera/problem2/products").map(line => line.split('|')).filter(r => !filt.contains(r(4))).map(r => (r(0).toInt,r(1).toInt,r(2),r(3),r(4).toFloat,r(5)))
// a) Dataframes
val productsDF = products.toDF("id","category_id","name","desc","price","image").filter("price < 100").orderBy(col("category_id").asc)
val resultDF = productsDF.groupBy(col("category_id")).agg(max("price").as("max_price"),count("id").as("total_products"),round(avg("price"),2).as("avg_price"),min("price").as("min_price"))
// b) SparkSQL
val productsSQL = products.toDF("id","category_id","name","desc","price","image").filter("price < 100").orderBy(col("category_id").asc)
productsSQL.registerTempTable("products")
val resultSQL = sqlContext.sql("""SELECT category_id, MAX(price) AS max_price,COUNT(id) AS total_products, ROUND(AVG(price),2) AS avg_price, MIN(price) AS min_price FROM products GROUP BY category_id""")
// c) SparkRDD
val productsRDD = products.filter(r => r._5 < 100).map(r => (r._2,(r._5,1))).sortByKey()
val aggByKey = productsRDD.aggregateByKey( (0.0F,0,0.0F,9999.9F) )( ( (i:(Float,Int,Float,Float),v:(Float, Int)) => (i._1.max(v._1),i._2 + v._2,i._3 + v._1,i._4.min(v._1)) ), ( (v:(Float,Int,Float,Float),c:(Float,Int,Float,Float)) => (v._1.max(c._1),v._2 + c._2,v._3 + c._3,v._4.min(c._4)) )).sortByKey().map({case((c, (m, t, a, mi))) => (c,m,t,a/t,mi)}).toDF("category_id","max_price","total_products","avg_price","min_price")
val resultRDD = aggByKey.selectExpr("category_id","max_price","total_products","round(avg_price,2) as avg_price","min_price")

// 5. store the result in avro file using snappy compression under these folders respectively
import com.databricks.spark.avro._
sqlContext.setConf("spark.sql.avro.compression.codec","snappy")
//	- /user/cloudera/problem2/products/result-df
resultDF.write.avro("/user/cloudera/problem2/products/result-df")
//	- /user/cloudera/problem2/products/result-sql
resultSQL.write.avro("/user/cloudera/problem2/products/result-sql")
//	- /user/cloudera/problem2/products/result-rdd
resultRDD.write.avro("/user/cloudera/problem2/products/result-rdd")

// 6.Check the output
$ hdfs dfs -ls /user/cloudera/problem2/products/result-df
$ avro-tools getmeta hdfs://quickstart.cloudera/user/cloudera/problem2/products/result-df/part-r-00000-1c3dd5a5-1a22-4a87-b837-6c3d1d64606f.avro
$ hdfs dfs -text /user/cloudera/problem2/products/result-df/part-r-00000-1c3dd5a5-1a22-4a87-b837-6c3d1d64606f.avro

$ hdfs dfs -ls /user/cloudera/problem2/products/result-sql
$ avro-tools getmeta hdfs://quickstart.cloudera/user/cloudera/problem2/products/result-sql/part-r-00000-6fc26667-e2e1-441f-b640-635be7c7a560.avro
$ hdfs dfs -text /user/cloudera/problem2/products/result-sql/part-r-00000-6fc26667-e2e1-441f-b640-635be7c7a560.avro

$ hdfs dfs -ls /user/cloudera/problem2/products/result-rdd
$ avro-tools getmeta hdfs://quickstart.cloudera/user/cloudera/problem2/products/result-rdd/part-r-00000-6e5b2247-7b44-4449-b30b-3f5a64bd7a95.avro
$ hdfs dfs -text /user/cloudera/problem2/products/result-rdd/part-r-00000-6e5b2247-7b44-4449-b30b-3f5a64bd7a95.avro