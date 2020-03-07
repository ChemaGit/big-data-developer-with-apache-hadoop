/** Question 98
  *   	- Task 1: Get revenue for given order_item_order_id
  *       	- Define function getOrderRevenue with 2 arguments order_items and order_id
  *       	- Use map reduce APIs to filter order items for given order id, to extract order_item_subtotal and add it to get revenue
  *       	- Return order revenue
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

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

object question98 {

  val spark = SparkSession
    .builder()
    .appName("question98")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question98")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val inputpath = "hdfs://quickstart.cloudera/public/retail_db/order_items"

  def getOrderRevenue(orderId: Int, orderItems: RDD[(Int, Double)]): Double = {
    orderItems
      .filter({case(id, subtotal) => id == orderId})
      .map({case(id, subtotal) => subtotal})
      .reduce((v1, v2) => v1 + v2)
  }

  def checkOrderId(s: String): Int = Try(s.toInt) match {
    case Success(n) => {
      if(n > 0) n else 0
    }
    case Failure(_) => 0
  }

  def main(args: Array[String]): Unit = {

    try {
      println("Introduce an order_id: ")
      val orderId = checkOrderId(scala.io.StdIn.readLine())
      if (orderId > 0) {
        println(s"order_id: $orderId")

        Logger.getRootLogger.setLevel(Level.ERROR)

        val order_items = sc
          .textFile(inputpath)
          .map(line => line.split(""","""))
          .map(r => (r(1).toInt,r(4).toDouble))

        val orderRevenue = getOrderRevenue(orderId,order_items)

        println(s"The total revenue for order_id $orderId is: $orderRevenue")

        println("Type whatever to the console to exit......")
        scala.io.StdIn.readLine()
      } else println(s"Invalid order_id $orderId, it must be a number greater than 0")
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
  --username retail_dba \
  --password cloudera \
  --table order_items \
  --as-textfile \
  --delete-target-dir \
  --target-dir /user/cloudera/question98/order_items \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

val orderItems = sc.textFile("/user/cloudera/question98/order_items").map(line => line.split(","))

def getOrderRevenue(order_items: org.apache.spark.rdd.RDD[Array[String]], order_id: Int): Double = {
  val filtered = order_items.filter(arr => arr(1).toInt == order_id)
  val result = filtered.map(arr => arr(4).toDouble).reduce( (v,c) => v + c)
  result
}

val res = getOrderRevenue(orderItems,2)
*/