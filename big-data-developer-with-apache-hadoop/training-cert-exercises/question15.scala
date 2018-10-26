/**
 * Problem Scenario 80 : You have been given MySQL DB with following details.
 * user=retail_dba
 * password=cloudera
 * database=retail_db
 * table=retail_db.products
 * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
 * Columns of products table : (product_id | product_category_id | product_name |
 * product_description | product_price | product_image )
 * Please accomplish following activities.
 * 1. Copy "retaildb.products" table to hdfs in a directory p93_products
 * 2. Now sort the products data sorted by product price per category, use productcategoryid
 * colunm to group by category
 */
//Step 1
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba --password cloudera \
--table products \
--null-non-string "-1" \
--delete-target-dir \
--target-dir p93_products \
--num-mappers 1-

//Step 2
hdfs dfs -ls p93_products
hdfs dfs -cat p93_products/part-m-00000

//Python
//Step 3 : Load this directory as RDD
productsRDD = sc.textFile("p93_products") 
//Step 4 : Filter empty prices, if exists #filter out empty prices lines 
nonempty_lines = productsRDD.filter(lambda x: float(x.split(",")[4]) > 0) 
//Step 5 : Create data set like (categroyld, (id,name,price) 
mappedRDD = nonempty_lines.map(lambda line: (line.split(",")[1], (line.split(",")[0], line.split(",")[2], float(line.split(",")[4])))) 
//Step 6 : Now groupBy the all records based on categoryld, which a key on mappedRDD it will produce output like (categoryld, iterable of all lines for a key/categoryld) 
groupByCategroyld = mappedRDD.groupByKey() 
for line in groupByCategroyld.collect(): print(line) 
//step 7 : Now sort the data in each category based on price in ascending order. # sorted is a function to sort an iterable, we can also specify, what would be the Key on which we want to sort 
//what would be the Key on which we want to sort, in this case we have price on which it needs to be sorted. 
groupByCategroyld.map(lambda tuple: sorted(tuple[1], key=lambda tupleValue: tupleValue[2])).take(5) 
//Step 8 : Now sort the data in each category based on price in descending order. # sorted is a function to sort an iterable, we can also specify, 
//what would be the Key on which we want to sort, in this case we have price which it needs to be sorted. on 
groupByCategroyld.map(lambda tuple: sorted(tuple[1], key=lambda tupleValue: tupleValue[2] , reverse=True)).take(5)

//Scala
//Step 3 : Load this directory as RDD
val productsRDD = sc.textFile("p93_products")
//Step 4 : Filter empty prices, if exists #filter out empty prices lines 
val noEmptyLines = productsRDD.map(l => l.split(",")(4).toFloat > 0)
//Step 5 : Create data set like (categroyld, (id,name,price) 
val dataSet = noEmptyLines.map(line => line.split(",")).map(arr => (arr(1),(arr(0),arr(2), arr(4).toFloat)))
//Step 6 : Now groupBy the all records based on categoryld, which a key on mappedRDD it will produce output like (categoryld, iterable of all lines for a key/categoryld) 
val group = dataSet.groupByKey()
group.foreach({t => println(t._1 + "," + t._2.mkString(","))})
//step 7 : Now sort the data in each category based on price in ascending order. # sorted is a function to sort an iterable, we can also specify, what would be the Key on which we want to sort 
//what would be the Key on which we want to sort, in this case we have price on which it needs to be sorted. 
val sortAsc = group.map({case(k,it) => (k,it.toList.sortWith({ case((id,n,p),(id1,n1,p1)) => p < p1}))})
sortAsc.foreach({t => println(t._1 + "," + t._2.mkString(","))})
//Step 8 : Now sort the data in each category based on price in descending order. # sorted is a function to sort an iterable, we can also specify, 
//what would be the Key on which we want to sort, in this case we have price which it needs to be sorted. on 
val sortDesc = group.map({case(k,it) => (k,it.toList.sortWith({ case((id,n,p),(id1,n1,p1)) => p > p1}))})
sortDesc.foreach({t => println(t._1 + "," + t._2.mkString(","))})