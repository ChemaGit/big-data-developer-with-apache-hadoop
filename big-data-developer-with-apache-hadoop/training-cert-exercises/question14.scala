/** Question 14
  * Problem Scenario 79 : You have been given MySQL DB with following details.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * table=retail_db.products
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Columns of products table : (product_id | product categoryid | product_name | product_description | product_prtce | product_image )
  * Please accomplish following activities.
  * 1. Copy "retaildb.products" table to hdfs in a directory question14/products
  * 2. Filter out all the empty prices
  * 3. Sort all the products based on price in both ascending as well as descending order.
  * 4. Sort all the products based on price as well as product_category_id in descending order.
  * 5. Use the below functions to do data ordering or ranking and fetch top 10 elements top() takeOrdered() sortByKey()
  */
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table products \
  --delete-target-dir \
  --target-dir /user/cloudera/question14/products \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

val filt = List("", " ")
val products = sc.textFile("/user/cloudera/question14/products").map(line => line.split(",")).filter(r => !filt.contains(r(4)))

val sortAsc = products.sortBy(r => r(4).toFloat)
sortAsc.collect.foreach(r => println(r.mkString(",")))

val sortDesc = products.sortBy(r => -r(4).toFloat)
sortDesc.collect.foreach(r => println(r.mkString(",")))

val sortCatPrice = products.map(r => ( (r(1).toInt,r(4).toFloat), r)).sortByKey(false)
sortCatPrice.collect.foreach(r => println(r._1 + "--" + r._2.mkString(",")))

val sortTop = products.map(r => ( (r(1).toInt,r(4).toFloat), r.mkString(","))).top(10)
sortTop.foreach(t => println("%d-%f ==> %s".format(t._1._1,t._1._2,t._2)))

val sortTakeOrdered = products.map(r => ( (r(1).toInt,r(4).toFloat), r.mkString(","))).takeOrdered(10)
sortTakeOrdered.foreach(t => println("%d-%f ==> %s".format(t._1._1,t._1._2,t._2)))

val sortByKeyDesc = products.map(r => ( (r(1).toInt,r(4).toFloat), r.mkString(","))).sortByKey(false)
sortByKeyDesc.take(10).foreach(r => println(r._1 + "--" + r._2))

val sortByKeyAsc = products.map(r => ( (r(1).toInt,r(4).toFloat), r.mkString(","))).sortByKey()
sortByKeyAsc.take(10).foreach(r => println(r._1 + "--" + r._2))