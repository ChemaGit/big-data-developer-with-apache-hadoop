/** Question 90
  * Problem Scenario 75 : You have been given MySQL DB with following details.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * table=retail_db.orders
  * table=retail_db.order_items
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Please accomplish following activities.
  * 1. Copy "retail_db.order_items" table to hdfs in respective directory question90/order_items .
  * 2. Do the summation of entire revenue in this table using scala
  * 3. Find the maximum and minimum revenue as well.
  * 4. Calculate average revenue
  * Columns of orde_items table : (order_item_id , order_item_order_id ,order_item_product_id, order_item_quantity,order_item_subtotal,order_item_product_price)
  * Save results in one file, in directory /user/cloudera/question90
  *
  * $ sqoop import \
  * --connect jdbc:mysql://quickstart.cloudera/retail_db \
  * --username retail_dba \
  * --password cloudera \
  * --table order_items \
  * --as-textfile \
  * --delete-target-dir \
  * --target-dir /user/cloudera/tables/order_items \
  * --bindir /home/cloudera/bindir \
  * --outdir /home/cloudera/outdir
  *
  * $ hdfs dfs -ls /user/cloudera/tables/order_items/
  * $ hdfs dfs -cat /user/cloudera/tables/order_items/part* | head -n 20
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object question90 {

  val spark = SparkSession
    .builder()
    .appName("question90")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question90")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val input = "hdfs://quickstart.cloudera/user/cloudera/tables/order_items/"
  val output = "hdfs://quickstart.cloudera/user/cloudera/exercises/question_90/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      /**
        * SPARK RDD
        */
      val order_items_rdd = sc
        .textFile(input)
        .map(line => line.split(","))
        .map(arr => arr(4).toDouble)
        .cache()

      val sumTotalRevenue = order_items_rdd.sum()
      val maxRevenue = order_items_rdd.max()
      val minRevenue = order_items_rdd.min()
      val averageRevenue = order_items_rdd.sum() / order_items_rdd.count()

      println(s"Total revenue: $sumTotalRevenue")
      println(s"Max Revenue: $maxRevenue")
      println(s"Min Revenue: $minRevenue")
      println(s"Average Revenue: $averageRevenue")

      sc
        .parallelize(List(s"Total revenue: $sumTotalRevenue", s"Max Revenue: $maxRevenue",s"Min Revenue: $minRevenue",s"Average Revenue: $averageRevenue"))
        .saveAsTextFile(s"${output}rdd")

      /**
        * SPARK DATAFRAMES
        */

      import spark.implicits._

      val order_items_df = order_items_rdd.toDF("revenue")
      order_items_df.createOrReplaceTempView("t_revenue")

      sqlContext
        .sql(
          """SELECT ROUND(SUM(revenue),2) AS SumRevenue, MAX(revenue) AS MaxRevenue, MIN(revenue) AS MinRevenue, ROUND(AVG(revenue),2) AS AverageRevenue
            |FROM t_revenue""".stripMargin)
        .rdd
        .saveAsTextFile(s"${output}dataframes")

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
val orderItems = sc.textFile("/user/cloudera/question90/order_items").map(line => line.split(",")).map(r => (r(0).toInt,r(1).toInt,r(2).toInt,r(3).toInt,r(4).toFloat,r(5).toFloat))

// SPARK RDD SOLUTION
val orderItemsRdd = orderItems.map(t => (t._5,1))
val agg = orderItemsRdd.aggregate(0.0F,0.0F,9999.0F,0)( ( (i:(Float,Float,Float,Int),v:(Float,Int)) => (i._1 + v._1,i._2.max(v._1),i._3.min(v._1),i._4 + v._2) ), ( (v:(Float,Float,Float,Int),c:(Float,Float,Float,Int)) => (v._1 + c._1,v._2.max(c._2),v._3.min(c._3),v._4 + c._4) ))
val format = sc.parallelize(List(agg)).map({case(sum,max,min,total) => (sum,max,min,sum / total)}).map({case(sum,max,min,avg) => "%f,%f,%f,%f".format(sum,max,min,avg)})
format.repartition(1).saveAsTextFile("/user/cloudera/question90/order_items/result_rdd")

$ hdfs dfs -cat /user/cloudera/question90/order_items/result_rdd/par*
// 34326256.000000,1999.989990,9.990000,199.341782

// SPARK SQL SOLUTION
val orderItemsDF = orderItems.toDF("order_item_id" , "order_item_order_id" ,"order_item_product_id", "order_item_quantity","order_item_subtotal","order_item_product_price")
orderItemsDF.registerTempTable("order_items")
val resultSql = sqlContext.sql("""SELECT SUM(order_item_subtotal),max(order_item_subtotal),min(order_item_subtotal),avg(order_item_subtotal) FROM order_items""")
resultSql.rdd.map(r => r.mkString(",")).saveAsTextFile("/user/cloudera/question90/order_items/result_sql")

$ hdfs dfs -cat /user/cloudera/question90/order_items/result_sql/par*
// 3.432262059842491E7,1999.99,9.99,199.32066922046081
*/