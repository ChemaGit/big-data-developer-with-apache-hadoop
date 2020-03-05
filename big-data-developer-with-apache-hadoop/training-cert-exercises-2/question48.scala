import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

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

/*
sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders \
--as-textfile \
--delete-target-dir \
--target-dir /user/cloudera/exercise_9/orders \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hdfs dfs -ls /user/cloudera/exercise_9/orders
$ hdfs dfs -text /user/cloudera/exercise_9/orders/part-m* | head -n 20
 */

object question48 {

  val spark = SparkSession
    .builder()
    .appName("question48")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question48")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val inputPath = "hdfs://quickstart.cloudera/user/cloudera/exercise_9/orders"
  val outputPath = "hdfs://quickstart.cloudera/user/cloudera/exercise_9/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val orders = sc.textFile(inputPath)
        .map(line => line.split(","))
        .map(arr => (arr(3),1))
        .cache()

      // count by key
      val countByKey = orders.countByKey()
      countByKey.foreach(println)

      sc.parallelize(countByKey.toList)
        .sortBy(t => t._2, false)
        .saveAsTextFile(s"${outputPath}count_by_key")

      println("*********************")

      // group by key
      val groupByKey = orders.groupByKey()
        .map(t => (t._1,t._2.sum))
        .sortBy(t => t._2, false)

      groupByKey
        .collect
        .foreach(println)

      groupByKey
        .saveAsTextFile(s"${outputPath}group_by_key")

      println("*********************")

      // reduce by key
      val reduceByKey = orders
        .reduceByKey( (v,c) => v + c)
        .sortBy(t => t._2, false)

      reduceByKey
        .collect
        .foreach(println)

      reduceByKey
        .saveAsTextFile(s"${outputPath}reduce_by_key")

      println("*********************")

      // aggregate by key
      val aggregateByKey = orders
        .aggregateByKey(0)(((z: Int,v: Int) => z + v), ((v: Int, c: Int) => v + c))
        .sortBy(t => t._2, false)

      aggregateByKey
        .collect
        .foreach(println)

      aggregateByKey
        .saveAsTextFile(s"${outputPath}aggregate_by_key")

      println("*********************")

      // combine by key
      val combineByKey = orders
        .combineByKey(((v: Int) => v), ((v: Int, c: Int) => v + c), ((v: Int, c: Int) => v + c))
        .sortBy(t => t._2, false)

      combineByKey
        .collect
        .foreach(println)

      combineByKey
        .saveAsTextFile(s"${outputPath}combine_by_key")

      // To have the opportunity to view the web console of Spark: http://localhost:4040/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("SparkContext stopped.")
      spark.stop()
      println("SparkSession stopped.")
    }
  }
}


/*SOLUTION IN THE SPARK REPL
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
--num-mappers 8

val orders = sc.textFile("/user/cloudera/question48/orders").map(line => line.split(",")).map(r => (r(3), 1))
val countByKey = orders.countByKey()
countByKey.foreach(println)
sc.parallelize(countByKey.toList).repartition(1).saveAsTextFile("/user/cloudera/question48/countByKey")

val groupByKey = orders.groupByKey().mapValues(v => v.size)
groupByKey.collect.foreach(println)
groupByKey.saveAsTextFile("/user/cloudera/question48/groupByKey")

val reduceByKey = orders.reduceByKey( (v,c) => v + c)
reduceByKey.collect.foreach(println)
reduceByKey.repartition(1).saveAsTextFile("/user/cloudera/question48/reduceByKey")

val aggregateByKey = orders.aggregateByKey(0)( ( (z: Int,v: Int) =>  z + v) , ( (c: Int, v: Int) => c + v) )
aggregateByKey.collect.foreach(println)
aggregateByKey.saveAsTextFile("/user/cloudera/question48/aggregateByKey")

val combineByKey = orders.combineByKey( ( (v: Int) => v) , ( (v: Int, c: Int) => v + c) , ( (v: Int, c: Int) => v + c))
combineByKey.collect.foreach(println)
combineByKey.saveAsTextFile("/user/cloudera/question48/combineByKey")

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
*/