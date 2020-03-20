/**
Question 1:
Instructions:
Connect to mySQL database using sqoop, import all orders whose id > 1000 into HDFS directory in gzip codec
Data Description:
A mysql instance is running on the gateway node.In that instance you will find orders table that contains orders data.
> Installation : on the cluser node gateway
> Database name: retail_db
> Table name: Orders
> Username: root
> Password: cloudera

Output Requirement:
Place the orders files in HDFS directory "/user/cloudera/problem1/orders_new/parquetdata"
Use parquet format with tab delimiter and compressed with gzip codec

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username root \
--password cloudera \
--table orders \
--where "order_id > 1000" \
--as-parquetfile \
--delete-target-dir \
--target-dir /user/cloudera/problem1/orders_new/parquet \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Problem1 {

  val spark = SparkSession
    .builder()
    .appName("Problem1")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "Problem1")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/problem1/orders_new/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val orders = sqlContext
        .read
        .parquet(s"${path}parquet")
        .cache()

      orders.show(10)

      orders
        .write
        .mode("overwrite")
        .option("compression","gzip")
        .parquet(s"${path}parquetdata")

      // TODO: check the results
      // hdfs dfs -ls /user/cloudera/problem1/orders_new/
      // hdfs dfs -ls /user/cloudera/problem1/orders_new/parquetdata
      // parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/problem1/orders_new/parquetdata/part-00003-1ad6be25-7f07-4344-b69b-c8977231c804-c000.gz.parquet | head -n 20

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
--where "order_id > 1000" \
--as-parquetfile \
--compress \
--compression-codec org.apache.hadoop.io.compress.GzipCodec \
--delete-target-dir \
--target-dir /user/cloudera/problem1/orders_new/parquet \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

val orders = sqlContext.read.parquet("/user/cloudera/problem1/orders_new/parquet")
orders.show()
sqlContext.setConf("spark.sql.parquet.compression.codec","gzip")
orders.write.parquet("/user/cloudera/problem1/orders_new/parquetdata")

$ hdfs dfs -ls /user/cloudera/problem1/orders_new/parquetdata
$ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/problem1/orders_new/parquetdata/part-r-00000-d5c9e54b-4b9d-4653-aa75-914b34aca8ae.gz.parquet
$ parquet-tools head hdfs://quickstart.cloudera/user/cloudera/problem1/orders_new/parquetdata/part-r-00000-d5c9e54b-4b9d-4653-aa75-914b34aca8ae.gz.parquet
*/