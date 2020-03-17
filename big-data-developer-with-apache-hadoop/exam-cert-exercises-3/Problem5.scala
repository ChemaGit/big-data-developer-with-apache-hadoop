/**
Question 5: Correct
PreRequiste:
[PreRequiste will not be there in actual exam]
Run below sqoop command to import orders table from mysql into hdfs to the destination /user/cloudera/problem3/parquet as parquet file.

sqoop import \
--connect "jdbc:mysql://quickstart.cloudera/retail_db" \
--password cloudera \
--username root \
--table orders \
--as-parquetfile \
--target-dir /user/cloudera/problem3/parquet \
--delete-target-dir \
--bindir /home/cloudera/bindir \
--outdir /home/cloudera/outdir

$ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/problem3/parquet/156d81e9-a17e-41f6-83a5-9706694b7ef6.parquet | head -n 10

Instructions:
Fetch all pending orders from data-files stored at hdfs location /user/cloudera/problem3/parquet and save it into json file in HDFS

Output Requirement:
Result should be saved in /user/cloudera/problem3/orders_pending
Output file should be saved as json file.
Output file should Gzip compressed.

Important Information:
Please make sure you are running all your solutions on spark 1.6 since exam environment will be providing that.
*/

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Problem5 {

  val spark = SparkSession
    .builder()
    .appName("Problem5")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "Problem5")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/problem3/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val ordersPending = sqlContext
        .read
        .parquet((s"${path}parquet"))
        .where("order_status LIKE('%PENDING%')")
        .cache

      ordersPending.show(10)

      ordersPending
        .toJSON
        .write
        .option("compression","gzip")
        .save(s"${path}orders_pending")

      // todo: check the results
      // hdfs dfs -ls /user/cloudera/problem3/orders_pending
      // parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/problem3/orders_pending/part-00000-62255d96-616a-41db-891a-b10be2d37c53-c000.gz.parquet | head -n 10

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
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table orders \
--as-parquetfile \
--delete-target-dir \
--target-dir /user/cloudera/problem3/parquet \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

val orders = sqlContext.read.parquet("/user/cloudera/problem3/parquet")
val pending = orders.filter("order_status like('%PENDING%')")
pending.toJSON.saveAsTextFile("/user/cloudera/problem3/orders_pending",classOf[org.apache.hadoop.io.compress.GzipCodec])

$ hdfs dfs -ls /user/cloudera/problem3/orders_pending
$ hdfs dfs -text /user/cloudera/problem3/orders_pending/part-00000.gz | tail -n 20
*/