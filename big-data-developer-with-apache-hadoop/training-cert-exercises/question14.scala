/**
 * Problem Scenario 79 : You have been given MySQL DB with following details.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * table=retail_db.orders
 * table=retail_db.order_items
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Columns of products table : (product_id | product_categoryid | product_name | product_description | product_price | product_image )
 * Please accomplish following activities.
 * 1. Copy "retaildb.products" table to hdfs in a directory p93_products
 * 2. Filter out all the empty prices
 * 3. Sort all the products based on price in both ascending as well as descending order.
 * 4. Sort all the products based on price as well as product_id in descending order.
 * 5. Use the below functions to do data ordering or ranking and fetch top 10 elements top() takeOrdered() sortByKey()
 */
//Step 1
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba --password cloudera \
--table products \
--target-dir p93_products \
--num-mappers 1

$ hdfs dfs -cat /user/cloudera/p93_products/part-m-00000

//Python
productsRDD = sc.textFile("p93_products/*")
//Step 4 : Filter empty prices, if exists #filter out empty prices lines
nonEmptyLines = productsRDD.filter(lambda x: len(x.split(",")[4]) > 0 )
//Step 5 : Now sort data based on product_price in order.
sortedPriceProducts = nonEmptyLines.map(lambda line: (float(line.split(",")[4]), line.split(",")[2])).sortByKey()
for line in sortedPriceProducts.collect(): print(line)
//Step 6 : Now sort data based on product_price in descending order. 
sortedPriceProducts = nonEmptyLines.map(lambda line: (float(line.split(",")[4]), line.split(",")[2])).sortByKey(False)
for line in sortedPriceProducts.collect(): print(line)
//Step 7 : Get highest price products name.
sortedPriceProducts = nonEmptyLines.map(lambda line: (float(line.split(",")[4]), line.split(",")[2] ) ).sortByKey(False).take(1)
print(sortedPriceProducts)
//Other way to achieve Step 7
sortedPriceProducts = nonEmptyLines.map(lambda line: (float(line.split(",")[4]), line.split(",")[2] ) ).sortByKey(False).first()
print(sortedPriceProducts)
//Step 8 : Now sort data based on product_price as well as product_id in descending order.
sortedPriceProducts = nonEmptyLines.map(lambda line: ( (float(line.split(",")[4]),int(line.split(",")[0])), line.split(",")[2]) ).sortByKey(False)
for line in sortedPriceProducts.collect(): print(line)
//Step 9 : Now sort data based on product_price as well as product_id in descending order, using top() function.
sortedPriceProducts = nonEmptyLines.map(lambda line: ( (float(line.split(",")[4]),int(line.split(",")[0])), line.split(",")[2]) ).top(nonEmptyLines.count())
for line in sortedPriceProducts: print(line)
//Step 10 : Now sort data based on product_price as ascending and product_id in ascending order, using takeOrdered{) function
sortedPriceProducts = nonEmptyLines.map(lambda line: ( (float(line.split(",")[4]),int(line.split(",")[0])), line.split(",")[2]) ).takeOrdered(nonEmptyLines.count())
for line in sortedPriceProducts: print(line)

//Scala
val productsRDD = sc.textFile("p93_products/*")
//Step 4 : Filter empty prices, if exists #filter out empty prices lines
val nonEmptyLines = productsRDD.filter(x => x.split(",")(4).length > 0)
val rdd = nonEmptyLines.map(line => line.split(","))
val len = rdd.count().toInt
//Step 5 : Now sort data based on product_price in order.
val order = rdd.map(arr => (arr(4).toFloat, arr)).sortByKey()
for(e <- order.collect()) println(e)
//Step 6 : Now sort data based on product_price in descending order. 
val order = rdd.map(arr => (arr(4).toFloat, arr)).sortByKey(false)
for(e <- order.collect()) println(e)
//Step 7 : Get highest price products name.
val highest = rdd.map(arr => (arr(4).toFloat, arr(2))).sortByKey(false).first()
println(highest)
//Step 8 : Now sort data based on product_price as well as product_id in descending order.
val order = rdd.map(arr => ((arr(4).toFloat, arr(0).toInt), arr)).sortByKey(false)
for(e <- order.collect()) println(e)
//Step 9 : Now sort data based on product_price as well as product_id in descending order, using top() function.
implicit val sortImple = new Ordering[((Float, Int),Array[String] )] {
      override def compare(a: ((Float, Int),Array[String] ), b: ((Float, Int),Array[String] )) = {
            if(a._1._1 > b._1._1) {
               +1
            }else{
               -1
            }
          }
      }
val order = rdd.map(arr => ((arr(4).toFloat, arr(0).toInt), arr)).top(len)(sortImple)
for(e <- order) println(e)
//Step 10 : Now sort data based on product_price as ascending and product_id in ascending order, using takeOrdered{) function
val order = rdd.map(arr => ((arr(4).toFloat, arr(0).toInt), arr)).takeOrdered(len)
for(e <- order) println(e)