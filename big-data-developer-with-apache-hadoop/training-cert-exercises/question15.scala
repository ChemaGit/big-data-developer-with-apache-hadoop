/** Question 15
  * Problem Scenario 80 : You have been given MySQL DB with following details.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * table=retail_db.products
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Columns of products table : (product_id | product_category_id | product_name |
  * product_description | product_price | product_image )
  * Please accomplish following activities.
  * 1. Copy "retaildb.products" table to hdfs in a directory question15/products
  * 2. Now sort the products data sorted by product_price per category, use product_category_id
  * colunm to group by category
  */
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table products \
  --as-textfile \
  --delete-target-dir \
  --target-dir /user/cloudera/question15/products \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

val filt = List("", " ")
val products = sc.textFile("/user/cloudera/question15/products").map(line => line.split(",")).filter(r => !filt.contains(r(4))).map(r => ( (r(1).toInt,r(4).toFloat), r.mkString(",")))
val productsSorted = products.sortByKey(false)

productsSorted.collect.foreach(println)