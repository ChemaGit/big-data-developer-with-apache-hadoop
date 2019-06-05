/** Question 48
  * Problem Scenario 76 : You have been given MySQL DB with following details.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * table=retail_db.orders
  * table=retail_db.order_items
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Columns of order table : (orderid , order_date , ordercustomerid, order_status)
  * Columns of order_items table : (order_item_id , order_item_order_id ,order_item_product_id, order_item_quantity,order_item_subtotal,order_item_product_price)
  * Please accomplish following activities.
  * 1. Copy "retail_db.orders" table to hdfs in a directory question48/orders
  * 2. Once data is copied to hdfs, using spark-shell calculate the number of order for each status.
  * 3. Use all the following methods to calculate the number of order for each status. (You need to know all these functions and its behavior for real exam)
  * -countByKey()
  * -groupByKey()
  * -reduceByKey()
  * -aggregateByKey()
  * -combineByKey()
  */
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table orders \
  --as-textfile \
  --delete-target-dir \
  --target-dir /user/cloudera/question48/orders \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

val orders = sc.textFile("/user/cloudera/question48/orders").map(line => line.split(",")).map(r => (r(3), 1))
val countByKey = orders.countByKey()
countByKey.foreach(println)
sc.parallelize(countByKey.toList).repartition(1).saveAsTextFile("/user/cloudera/question48/countByKey")

val groupByKey = orders.groupByKey().mapValues(v => v.size)
groupByKey.collect.foreach(println)
groupByKey.repartition(1).saveAsTextFile("/user/cloudera/question48/groupByKey")

val reduceByKey = orders.reduceByKey( (v,c) => v + c)
reduceByKey.collect.foreach(println)
reduceByKey.repartition(1).saveAsTextFile("/user/cloudera/question48/reduceByKey")

val aggregateByKey = orders.aggregateByKey(0)( ( (z: Int,v: Int) =>  z + v) , ( (c: Int, v: Int) => c + v) )
aggregateByKey.collect.foreach(println)
aggregateByKey.repartition(1).saveAsTextFile("/user/cloudera/question48/aggregateByKey")

val combineByKey = orders.combineByKey( ( (v: Int) => v) , ( (v: Int, c: Int) => v + c) , ( (v: Int, c: Int) => v + c))
combineByKey.collect.foreach(println)
combineByKey.repartition(1).saveAsTextFile("/user/cloudera/question48/combineByKey")

$ hdfs dfs -ls /user/cloudera/question48/countByKey
$ hdfs dfs -cat /user/cloudera/question48/countByKey/part-00000

$ hdfs dfs -ls /user/cloudera/question48/groupByKey
$ hdfs dfs -cat /user/cloudera/question48/groupByKey/part-00000

$ hdfs dfs -ls /user/cloudera/question48/reduceByKey
$ hdfs dfs -cat /user/cloudera/question48/reduceByKey/part-00000

$ hdfs dfs -ls /user/cloudera/question48/aggregateByKey
$ hdfs dfs -cat /user/cloudera/question48/aggregateByKey/part-00000

$ hdfs dfs -ls /user/cloudera/question48/combineByKey
$ hdfs dfs -cat /user/cloudera/question48/combineByKey/part-00000