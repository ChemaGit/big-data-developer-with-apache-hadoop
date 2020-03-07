/** Question 99
  *   	- Task 2: Get revenue for each order_item_order_id
  *       	- Define function getRevenuePerOrder with 1 argument order_items
  *       	- Use map reduce APIs to get order_item_order_id and order_item_subtotal, then group by order_item_order_id and then process the values for each order_item_order_id
  *       	- Return a collection which contain order_item_order_id and revenue_per_order_id
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/*
+--------------------------+------------+------+-----+---------+----------------+
| Field                    | Type       | Null | Key | Default | Extra          |
+--------------------------+------------+------+-----+---------+----------------+
| order_item_id            | int(11)    | NO   | PRI | NULL    | auto_increment |
| order_item_order_id      | int(11)    | NO   |     | NULL    |                |
| order_item_product_id    | int(11)    | NO   |     | NULL    |                |
| order_item_quantity      | tinyint(4) | NO   |     | NULL    |                |
| order_item_subtotal      | float      | NO   |     | NULL    |                |
| order_item_product_price | float      | NO   |     | NULL    |                |
+--------------------------+------------+------+-----+---------+----------------+

sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
--username root \
--password cloudera \
--table order_items \
--as-textfile \
--target-dir /public/retail_db/order_items \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir
 */

object question99 {

  val spark = SparkSession
    .builder()
    .appName("question99")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question99")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val inputpath = "hdfs://quickstart.cloudera/public/retail_db/order_items"

  def getRevenuePerOrder(itemOrders: RDD[(Int,Double)]): Map[Int, Double] = {
    itemOrders
      .reduceByKey((v1,v2) => v1 + v2)
      .collect
      .toMap
  }

  def main(args: Array[String]): Unit = {
    try {

      Logger.getRootLogger.setLevel(Level.ERROR)

      val order_items = sc
        .textFile(inputpath)
        .map(line => line.split(""","""))
        .map(r => (r(1).toInt,r(4).toDouble))
        .cache()

      val mapOrderIdRevenue = getRevenuePerOrder(order_items)

      mapOrderIdRevenue.foreach(println)

      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("Stopped SparkContext")
      spark.stop()
      println("Stopped SparkSession")
    }
  }
}


/*SOLUTION IN THE SPARK REPL
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table order_items \
  --as-textfile \
  --delete-target-dir \
  --target-dir /user/cloudera/question99/order_items \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8


val orderItems = sc.textFile("/user/cloudera/question99/order_items").map(line => line.split(","))

def getOrderRevenue(order_items: org.apache.spark.rdd.RDD[Array[String]]): List[(Int, Double)] = {
  val tuple = order_items.map(arr => (arr(1).toInt, arr(4).toDouble))
  val reduce = tuple.reduceByKey( (v,c) => v + c).sortByKey()
  reduce.collect.toList
}

val res = getOrderRevenue(orderItems)
res.foreach(x => println(x))
*/