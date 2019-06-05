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

//1. Using sqoop copy data available in mysql products table to folder /user/cloudera/products on hdfs as text file. columns should be delimited by pipe '|'
sqoop import \
--connect jdbc:mysql://localhost/retail_db \
--username training \
--password training \
--table products \
--delete-target-dir \
--target-dir /user/cloudera/products \
--fields-terminated-by "|" \
--num-mappers 1

//2. move all the files from /user/cloudera/products folder to /user/cloudera/problem2/products folder
$ hdfs dfs -mkdir /user/cloudera/problem2/
$ hdfs dfs -mkdir /user/cloudera/problem2/products
$ hdfs dfs -mv /user/cloudera/products/p* /user/cloudera/problem2/products
$ hdfs dfs -cat /user/cloudera/problem2/products/p*

//3. Change permissions of all the files under /user/cloudera/problem2/products such that owner has read,write and execute permissions, group has read and write permissions whereas others have just read and execute permissions
$ hdfs dfs -chmod 765 /user/cloudera/problem2/products/p*

//4. read data in /user/cloudera/problem2/products and do the following operations using a) dataframes api b) spark sql c) RDDs aggregateByKey method. 
//Your solution should have three sets of steps. 
//Sort the resultant dataset by category id
//	- filter such that your RDD\DF has products whose price is lesser than 100 USD
//	- on the filtered data set find out the higest value in the product_price column under each category
//	- on the filtered data set also find out total products under each category
//	- on the filtered data set also find out the average price of the product under each category
//	- on the filtered data set also find out the minimum price of the product under each category
case class Product(productID: Int, productCategory: Int, productName: String, productDesc: String, productPrice: Float, productImage: String)
val products = sc.textFile("/user/training/problem2/products").map(line => line.split('|')) .map(arr => Product(arr(0).toInt,arr(1).toInt,arr(2),arr(3),arr(4).toFloat,arr(5)))

//a) dataframes api 
import org.apache.spark.sql.functions._
val productsDFApi = products.toDF
var dataFrameResult = productsDFApi.filter("productPrice < 100").groupBy(col("productCategory")).agg(max(col("productPrice")).alias("max_price"),
                                    countDistinct(col("productID")).alias("tot_products"),
                                    round(avg(col("productPrice")),2).alias("avg_price"),
                                    min(col("productPrice")).alias("min_price")).orderBy(col("productCategory"))
dataFrameResult.show()

//b) spark sql 
val productsDFSql = products.toDF
productsDFSql.registerTempTable("products")
val sqlResult = sqlContext.sql("SELECT productCategory, max(productPrice) AS `MAX PRICE`, count(DISTINCT(productID)) AS `TOTAL PROD`, CAST(avg(productPrice) AS DECIMAL(10,2)) AS `AVERAGE`, min(productPrice) AS `MIN PRICE` FROM products WHERE productPrice < 100 GROUP BY productCategory ORDER BY productCategory")

sqlResult.show()

//c) RDDs aggregateByKey
val productsDF = products.toDF
var rddResult = productsDF.map(x=>(x(1).toString.toInt,x(4).toString.toDouble)).aggregateByKey((0.0,0.0,0,9999999999999.0))((x,y)=>(math.max(x._1,y),x._2+y,x._3+1,math.min(x._4,y)),(x,y)=>(math.max(x._1,y._1),x._2+y._2,x._3+y._3,math.min(x._4,y._4))).map(x=> (x._1,x._2._1,(x._2._2/x._2._3),x._2._3,x._2._4)).sortBy(_._1, false);
rddResult.collect().foreach(println);

//5. store the result in avro file using snappy compression under these folders respectively
import com.databricks.spark.avro._
sqlContext.setConf("spark.sql.avro.compression.codec","snappy")
	//- /user/cloudera/problem2/products/result-df
dataFrameResult.write.avro("/user/cloudera/problem2/products/result-df")
	//- /user/cloudera/problem2/products/result-sql
sqlResult.write.avro("/user/cloudera/problem2/products/result-sql")
	//- /user/cloudera/problem2/products/result-rdd
rddResult.toDF().write.avro("/user/cloudera/problem2/products/result-rdd")