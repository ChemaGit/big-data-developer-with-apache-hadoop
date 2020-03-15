/**
Problem 1:
1.Using sqoop, import orders table into hdfs to folders /user/cloudera/problem1/orders. File should be loaded as Avro File and use snappy compression
2.Using sqoop, import order_items  table into hdfs to folders /user/cloudera/problem1/order-items. Files should be loaded as avro file and use snappy compression
3.Using Spark Scala load data at /user/cloudera/problem1/orders and /user/cloudera/problem1/orders-items items as dataframes.
4.Expected Intermediate Result: Order_Date , Order_status, total_orders, total_amount. In plain english, please find total orders and total amount per status per day.
The result should be sorted by order date in descending, order status in ascending and total amount in descending and total orders in ascending.
Aggregation should be done using below methods. However, sorting can be done using a dataframe or RDD. Perform aggregation in each of the following ways
a). Just by using Data Frames API - here order_date should be YYYY-MM-DD format
b). Using Spark SQL  - here order_date should be YYYY-MM-DD format
c). By using combineByKey function on RDDS -- No need of formatting order_date or total_amount
5.Store the result as parquet file into hdfs using gzip compression under folder
/user/cloudera/problem1/result4a-gzip
/user/cloudera/problem1/result4b-gzip
/user/cloudera/problem1/result4c-gzip
6.Store the result as parquet file into hdfs using snappy compression under folder
/user/cloudera/problem1/result4a-snappy
/user/cloudera/problem1/result4b-snappy
/user/cloudera/problem1/result4c-snappy
7.Store the result as CSV file into hdfs using No compression under folder
/user/cloudera/problem1/result4a-csv
/user/cloudera/problem1/result4b-csv
/user/cloudera/problem1/result4c-csv
8.create a mysql table named result and load data from /user/cloudera/problem1/result4a-csv to mysql table named result

  1.Using sqoop, import orders table into hdfs to folders /user/cloudera/problem1/orders. File should be loaded as Avro File and use snappy compression
$ sqoop import \
  --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
  --username retail_dba \
  --password cloudera \
  --table orders \
  --as-avrodatafile \
  --compress \
  --compression-codec snappy \
  --delete-target-dir \
  --target-dir /user/cloudera/problem1/orders \
  --outdir /home/cloudera/outdir \
  --bindir /home/cloudera/bindir

$ hdfs dfs -ls /user/cloudera/problem1/orders
$ hdfs dfs -text  /user/cloudera/problem1/orders/part-m-00000.avro | head -n 10
$ avro-tools getmeta hdfs://quickstart.cloudera/user/cloudera/problem1/orders/part-m-00000.avro

  2.Using sqoop, import order_items  table into hdfs to folders /user/cloudera/problem1/order-items. Files should be loaded as avro file and use snappy compression
$ sqoop import \
  --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
  --username retail_dba \
  --password cloudera \
  --table order_items \
  --as-avrodatafile \
  --compress \
  --compression-codec snappy \
  --delete-target-dir \
  --target-dir /user/cloudera/problem1/order-items \
  --outdir /home/cloudera/outdir \
  --bindir /home/cloudera/bindir

$ hdfs dfs -ls /user/cloudera/problem1/order-items
$ hdfs dfs -text /user/cloudera/problem1/order-items/part-m-00000.avro | head -n 10
$ avro-tools getmeta hdfs://quickstart.cloudera/user/cloudera/problem1/order-items/part-m-00000.avro
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object Problem_1 {

  val spark = SparkSession
    .builder()
    .appName("Problem_1")
    .master("local[*]")
    .enableHiveSupport()
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "Problem_1") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val inputpath = "hdfs://quickstart.cloudera/user/cloudera/problem1/"
  val output = "hdfs://quickstart.cloudera/user/cloudera/problem1/"

  def main(args: Array[String]): Unit = {

    try {

      Logger.getRootLogger.setLevel(Level.ERROR)

      // 3.Using Spark Scala load data at /user/cloudera/problem1/orders and /user/cloudera/problem1/orders-items items as dataframes.
      import com.databricks.spark.avro._

      val ordersDF = spark
        .sqlContext
        .read
        .avro(s"${inputpath}orders/")

      val orderItemsDF = spark
        .sqlContext
        .read
        .avro(s"${inputpath}order-items/")

      import spark.implicits._

      // 4.Expected Intermediate Result: Order_Date , Order_status, total_orders, total_amount. In plain english, please find total orders and total amount per status per day.
      //  The result should be sorted by order date in descending, order status in ascending and total amount in descending and total orders in ascending.
      // Aggregation should be done using below methods. However, sorting can be done using a dataframe or RDD. Perform aggregation in each of the following ways
      //  a). Just by using Data Frames API - here order_date should be YYYY-MM-DD format
      val joined = ordersDF
        .join(orderItemsDF, $"order_id" === $"order_item_order_id", "inner")
        .cache()

      val resultDF = joined
        .groupBy(col("order_date"), col("order_status"))
        .agg(countDistinct("order_id").as("total_orders"), round(sum("order_item_subtotal"), 2).as("total_amount"))
        .selectExpr("""from_unixtime(order_date / 1000,"yyyy-MM-dd") as order_date""", """order_status""", """total_orders""", """total_amount""")
        .orderBy(col("order_date").desc, col("order_status").asc, col("total_amount").desc, col("total_orders").asc)

      //  b). Using Spark SQL  - here order_date should be YYYY-MM-DD format
      joined.createOrReplaceTempView("joined")
      val resultSQL = spark
        .sqlContext
        .sql(
          """SELECT from_unixtime(order_date / 1000, "yyyy-MM-dd") AS order_date, order_status,
            |COUNT(DISTINCT(order_id)) AS total_orders, ROUND(SUM(order_item_subtotal),2) AS total_amount
            |FROM joined
            |GROUP BY order_date, order_status
            |ORDER BY order_date DESC,order_status ASC,total_amount DESC,total_orders ASC""".stripMargin)

      //  c). By using combineByKey function on RDDS -- No need of formatting order_date or total_amount
      val resultRDD = joined
        .rdd
        .map(r => ((r(1).toString.toLong, r(3).toString), (r(0).toString.toInt, r(8).toString.toDouble)))
        .combineByKey(((v: (Int, Double)) => (Set(v._1), v._2)), ((c: (Set[Int], Double), v: (Int, Double)) => (c._1 + v._1, c._2 + v._2)), ((c: (Set[Int], Double), v: (Set[Int], Double)) => (c._1 ++ v._1, c._2 + v._2)))
        .map({ case (((d, s), (to, ta))) => (d, s, to.size, ta) })
        .toDF("order_date", "order_status", "total_orders", "total_amount")
        .selectExpr("""from_unixtime(order_date / 1000,"yyyy-MM-dd") as order_date""", """order_status""", """total_orders""", """ROUND(total_amount,2) AS total_amount""")
        .orderBy(col("order_date").desc, col("order_status").asc, col("total_amount").desc, col("total_orders").asc)

      // 5.Store the result as parquet file into hdfs using gzip compression under folder
      spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "gzip")
      // /user/cloudera/problem1/result4a-gzip
      resultDF.write.parquet(s"${output}result4a-gzip")
      // /user/cloudera/problem1/result4b-gzip
      resultSQL.write.parquet(s"${output}result4b-gzip")
      // /user/cloudera/problem1/result4c-gzip
      resultRDD.write.parquet(s"${output}result4c-gzip")

      // 6.Store the result as parquet file into hdfs using snappy compression under folder
      spark
        .sqlContext
        .setConf("spark.sql.parquet.compression.codec", "snappy")
      // /user/cloudera/problem1/result4a-snappy
      resultDF.write.parquet(s"${output}result4a-snappy")
      // /user/cloudera/problem1/result4b-snappy
      resultSQL.write.parquet(s"${output}result4b-snappy")
      // /user/cloudera/problem1/result4c-snappy
      resultRDD.write.parquet(s"${output}result4c-snappy")

      // 7.Store the result as CSV file into hdfs using No compression under folder
      // /user/cloudera/problem1/result4a-csv
      resultDF
        .rdd
        .map(r => r.mkString(","))
        .saveAsTextFile(s"${output}result4a-csv")
      // /user/cloudera/problem1/result4b-csv
      resultSQL
        .rdd
        .map(r => r.mkString(","))
        .saveAsTextFile(s"${output}result4b-csv")
      // /user/cloudera/problem1/result4c-csv
      resultRDD
        .rdd
        .map(r => r.mkString(","))
        .saveAsTextFile(s"${output}result4c-csv")

      // 8.create a mysql table named result and load data from /user/cloudera/problem1/result4a-csv to mysql table named result
      /* Using Sqoop
      mysql -u root -p cloudera
      use retail_export
      CREATE TABLE result(order_date varchar(16),order_status varchar(16),total_orders int,total_amount double);

      sqoop export \
      --connect jdbc:mysql://quickstart.cloudera:3306/retail_export \
      --username root \
      --password cloudera \
      --table result \
      --export-dir /user/cloudera/problem1/result4a-csv \
      --input-fields-terminated-by ',' \
      --input-lines-terminated-by '\n' \
      --outdir /home/cloudera/outdir \
      --bindir /home/cloudera/bindir

      SELECT * FROM result LIMIT 10;
     */

      // 9.create a mysql table named result_jdbc and load data from dataframe resultSQL
      /*mysql -u root -p cloudera
        use retail_export
        CREATE TABLE result_jdbc(order_date varchar(16),order_status varchar(16),total_orders int,total_amount double);*/
      val props = new java.util.Properties()
      props.setProperty("user", "root")
      props.setProperty("password", "cloudera")
      resultSQL
        .repartition(1)
        .write
        .jdbc("jdbc:mysql://quicstart.cloudera:3306/retail_export", "result_jdbc", props)
      // mysql> SELECT * FROM result_jdbc LIMIT 10;

      // // 10.create a table in HIVE named result_hive and load data from /user/cloudera/problem1/result4a-snappy
      /*
      $ hive
      hive> use hadoopexam;
      hive> hive> CREATE TABLE result_hive(order_date string,order_status string,total_orders bigint,total_amount double)
                  STORED AS PARQUET
                  LOCATION '/user/cloudera/problem1/result4a-snappy'
                  TBLPROPERTIES("parquet.compression"="snappy");
      hive> select * from result_hive limit 10;
       */


      // To have the opportunity to view the web console of Spark: http://localhost:4040/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("Stopped SparkContext")
      spark.stop()
      println("Stopped SparkSession")
    }
  }

  // 11.check the results
  //  $ hdfs dfs -ls /user/cloudera/problem1/result4a-gzip
  //  $ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/problem1/result4a-gzip/part-r-00000-f9a543d3-45a8-4b82-9a25-572e242c9b77.gz.parquet
  //  $ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/problem1/result4a-gzip/part-r-00000-f9a543d3-45a8-4b82-9a25-572e242c9b77.gz.parquet
  //
  //  $ hdfs dfs -ls /user/cloudera/problem1/result4b-gzip
  //  $ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/problem1/result4b-gzip/part-r-00000-4576f39e-986e-4288-bef8-04d7e7f4de0f.gz.parquet
  //  $ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/problem1/result4b-gzip/part-r-00000-4576f39e-986e-4288-bef8-04d7e7f4de0f.gz.parquet
  //
  //  $ hdfs dfs -ls /user/cloudera/problem1/result4c-gzip
  //  $ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/problem1/result4c-gzip/part-r-00000-e02e07dd-29ee-4f95-b7a2-42304fc2e3bf.gz.parquet
  //  $ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/problem1/result4c-gzip/part-r-00000-e02e07dd-29ee-4f95-b7a2-42304fc2e3bf.gz.parquet
  //
  //  $ hdfs dfs -ls /user/cloudera/problem1/result4a-snappy
  //  $ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/problem1/result4a-snappy/part-r-00000-195923be-6ba4-409d-a360-efece6dd3b14.snappy.parquet
  //  $ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/problem1/result4a-snappy/part-r-00000-195923be-6ba4-409d-a360-efece6dd3b14.snappy.parquet
  //
  //  $ hdfs dfs -ls /user/cloudera/problem1/result4b-snappy
  //  $ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/problem1/result4b-snappy/part-r-00000-c4854798-bd0d-4f11-ade8-e33a269e2933.snappy.parquet
  //  $ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/problem1/result4b-snappy/part-r-00000-c4854798-bd0d-4f11-ade8-e33a269e2933.snappy.parquet
  //
  //  $ hdfs dfs -ls /user/cloudera/problem1/result4c-snappy
  //  $ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/problem1/result4c-snappy/part-r-00000-458400cc-8d04-4ba8-933b-84fefb9e5585.snappy.parquet
  //  $ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/problem1/result4c-snappy/part-r-00000-458400cc-8d04-4ba8-933b-84fefb9e5585.snappy.parquet
  //
  //  $ hdfs dfs -ls /user/cloudera/problem1/result4a-csv
  //  $ hdfs dfs -cat /user/cloudera/problem1/result4a-csv/part-00000 | head -n 10
  //
  //  $ hdfs dfs -ls /user/cloudera/problem1/result4b-csv
  //  $ hdfs dfs -cat /user/cloudera/problem1/result4b-csv/part-00000 | head -n 10
  //
  //  $ hdfs dfs -ls /user/cloudera/problem1/result4c-csv
  //  $ hdfs dfs -cat /user/cloudera/problem1/result4c-csv/part-00000 | head -n 10

}


/*SOLUTION IN THE SPARK REPL
// 1.Using sqoop, import orders table into hdfs to folders /user/cloudera/problem1/orders. File should be loaded as Avro File and use snappy compression
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table orders \
  --as-avrodatafile \
  --compress \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
  --delete-target-dir \
  --target-dir /user/cloudera/problem1/orders \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hdfs dfs -ls /user/cloudera/problem1/orders
$ avro-tools getmeta hdfs://quickstart.cloudera/user/cloudera/problem1/orders/part-m-00000.avro

// 2.Using sqoop, import order_items  table into hdfs to folders /user/cloudera/problem1/order-items. Files should be loaded as avro file and use snappy compression
sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table order_items \
  --as-avrodatafile \
  --compress \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
  --delete-target-dir \
  --target-dir /user/cloudera/problem1/order_items \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ hdfs dfs -ls /user/cloudera/problem1/order_items
$ avro-tools getmeta hdfs://quickstart.cloudera/user/cloudera/problem1/order_items/part-m-00000.avro

// 3.Using Spark Scala load data at /user/cloudera/problem1/orders and /user/cloudera/problem1/orders_items items as dataframes.
// 4.Expected Intermediate Result: Order_Date , Order_status, total_orders, total_amount.
//   In plain english, please find total orders and total amount per status per day.
//   The result should be sorted by order date in descending, order status in ascending and total amount in descending and total orders in ascending.
//   Aggregation should be done using below methods. However, sorting can be done   using a dataframe or RDD. Perform aggregation in each of the following ways
// a). Just by using Data Frames API - here order_date should be YYYY-MM-DD format
// b). Using Spark SQL  - here order_date should be YYYY-MM-DD format
// c). By using combineByKey function on RDDS -- No need of formatting order_date or total_amount

import com.databricks.spark.avro._
val orders = sqlContext.read.avro("/user/cloudera/problem1/orders")
val orderItems = sqlContext.read.avro("/user/cloudera/problem1/order_items")

// a). Just by using Data Frames API - here order_date should be YYYY-MM-DD format
val joinedDF = orders.join(orderItems,$"order_id"===$"order_item_order_id")
val resultDF = joinedDF.groupBy(column("order_date"),column("order_status")).agg(countDistinct("order_id").as("total_orders"), round(sum("order_item_subtotal"),2).as("total_amount")).selectExpr("from_unixtime(order_date / 1000,'yyyy-MM-dd') as order_date","order_status","total_orders","total_amount").orderBy(column("order_date").desc, column("order_status").asc,column("total_amount").desc,column("total_orders").asc)

// b). Using Spark SQL  - here order_date should be YYYY-MM-DD format
orders.registerTempTable("o")
orderItems.registerTempTable("oi")
val resultSQL = sqlContext.sql("""SELECT FROM_UNIXTIME(order_date/1000, "yyyy-MM-dd") AS order_date, order_status, COUNT(DISTINCT(order_id)) AS total_orders, ROUND(SUM(order_item_subtotal),2) AS total_amount FROM o JOIN oi ON(order_id = order_item_order_id) GROUP BY order_date, order_status ORDER BY order_date DESC, order_status, total_amount DESC, total_orders""")

// c). By using combineByKey function on RDDS -- No need of formatting order_date or total_amount
val joinedRDD = orders.join(orderItems,$"order_id"===$"order_item_order_id").rdd.map(r => ( (r(1).toString.toLong,r(3).toString), (r(0).toString.toInt,r(8).toString.toFloat)))
val combByKey = joinedRDD.combineByKey( ( (v:(Int,Float)) => (Set(v._1), v._2) ), ( (c:(Set[Int],Float),v:(Int,Float)) => (c._1 + v._1,c._2 + v._2)), ( (c:(Set[Int],Float),v:(Set[Int],Float)) => ( c._1 ++ v._1, c._2 + v._2)))
val rddDF = combByKey.map({ case(((d, s), (to, ta))) => (d,s,to.size,ta)}).toDF("order_date","order_status","total_orders","total_amount")
val resultRDD = rddDF.selectExpr("from_unixtime(order_date / 1000,'yyyy-MM-dd') as order_date","order_status","total_orders","ROUND(total_amount,2) as total_amount").orderBy(column("order_date").desc, column("order_status").asc,column("total_amount").desc,column("total_orders").asc)

// 5.Store the result as parquet file into hdfs using gzip compression under folder
sqlContext.setConf("spark.sql.parquet.compression.codec","gzip")
// /user/cloudera/problem1/result4a-gzip
resultDF.write.parquet("/user/cloudera/problem1/result4a-gzip")
// /user/cloudera/problem1/result4b-gzip
resultSQL.write.parquet("/user/cloudera/problem1/result4b-gzip")
// /user/cloudera/problem1/result4c-gzip
resultRDD.write.parquet("/user/cloudera/problem1/result4c-gzip")
// 6.Store the result as parquet file into hdfs using snappy compression under folder
sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
// /user/cloudera/problem1/result4a-snappy
resultDF.write.parquet("/user/cloudera/problem1/result4a-snappy")
// /user/cloudera/problem1/result4b-snappy
resultSQL.write.parquet("/user/cloudera/problem1/result4b-snappy")
// /user/cloudera/problem1/result4c-snappy
resultRDD.write.parquet("/user/cloudera/problem1/result4c-snappy")
// 7.Store the result as CSV file into hdfs using No compression under folder
// /user/cloudera/problem1/result4a-csv
resultDF.rdd.map(r => r.mkString(",")).saveAsTextFile("/user/cloudera/problem1/result4a-csv")
// /user/cloudera/problem1/result4b-csv
resultSQL.rdd.map(r => r.mkString(",")).saveAsTextFile("/user/cloudera/problem1/result4b-csv")
// /user/cloudera/problem1/result4c-csv
resultRDD.rdd.map(r => r.mkString(",")).saveAsTextFile("/user/cloudera/problem1/result4c-csv")
// 8.create a mysql table named result and load data from /user/cloudera/problem1/result4a-csv to mysql table named result
$ mysql -u root -p
mysql> use retail_db;
mysql> CREATE TABLE result(order_date varchar(32),order_status varchar(16),total_orders int,total_amount float);

sqoop export \
--connect jdbc:mysql://quickstart:3306/retail_db \
  --username root \
  --password cloudera \
  --table result \
  --export-dir /user/cloudera/problem1/result4a-csv \
  --input-fields-terminated-by ',' \
  --input-lines-terminated-by '\n' \
  --outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

mysql> select * from result limit 10;

// 9.create a mysql table named result_jdbc and load data from dataframe resultSQL
val props = new java.util.Properties()
props.setProperty("user", "root")
props.setProperty("password", "cloudera")
resultSQL.write.jdbc("jdbc:mysql://quickstart:3306/retail_db","result_jdbc", props)

$ mysql -u root -p
mysql> use retail_db;
mysql> select * from result_jdbc limit 10;

// 10.create a table in HIVE named result_hive and load data from /user/cloudera/problem1/result4a-snappy
$ hive
  hive> use hadoopexam;
hive> CREATE TABLE result_hive(order_date string,order_status string,total_orders bigint,total_amount double) STORED AS PARQUET LOCATION '/user/cloudera/problem1/result4a-snappy' TBLPROPERTIES("parquet.compression"="snappy");
hive> select * from result_hive limit 10;

// 11.check the results
$ hdfs dfs -ls /user/cloudera/problem1/result4a-gzip
$ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/problem1/result4a-gzip/part-r-00000-f9a543d3-45a8-4b82-9a25-572e242c9b77.gz.parquet
$ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/problem1/result4a-gzip/part-r-00000-f9a543d3-45a8-4b82-9a25-572e242c9b77.gz.parquet

$ hdfs dfs -ls /user/cloudera/problem1/result4b-gzip
$ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/problem1/result4b-gzip/part-r-00000-4576f39e-986e-4288-bef8-04d7e7f4de0f.gz.parquet
$ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/problem1/result4b-gzip/part-r-00000-4576f39e-986e-4288-bef8-04d7e7f4de0f.gz.parquet

$ hdfs dfs -ls /user/cloudera/problem1/result4c-gzip
$ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/problem1/result4c-gzip/part-r-00000-e02e07dd-29ee-4f95-b7a2-42304fc2e3bf.gz.parquet
$ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/problem1/result4c-gzip/part-r-00000-e02e07dd-29ee-4f95-b7a2-42304fc2e3bf.gz.parquet

$ hdfs dfs -ls /user/cloudera/problem1/result4a-snappy
$ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/problem1/result4a-snappy/part-r-00000-195923be-6ba4-409d-a360-efece6dd3b14.snappy.parquet
$ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/problem1/result4a-snappy/part-r-00000-195923be-6ba4-409d-a360-efece6dd3b14.snappy.parquet

$ hdfs dfs -ls /user/cloudera/problem1/result4b-snappy
$ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/problem1/result4b-snappy/part-r-00000-c4854798-bd0d-4f11-ade8-e33a269e2933.snappy.parquet
$ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/problem1/result4b-snappy/part-r-00000-c4854798-bd0d-4f11-ade8-e33a269e2933.snappy.parquet

$ hdfs dfs -ls /user/cloudera/problem1/result4c-snappy
$ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/problem1/result4c-snappy/part-r-00000-458400cc-8d04-4ba8-933b-84fefb9e5585.snappy.parquet
$ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/problem1/result4c-snappy/part-r-00000-458400cc-8d04-4ba8-933b-84fefb9e5585.snappy.parquet

$ hdfs dfs -ls /user/cloudera/problem1/result4a-csv
$ hdfs dfs -cat /user/cloudera/problem1/result4a-csv/part-00000 | head -n 10

$ hdfs dfs -ls /user/cloudera/problem1/result4b-csv
$ hdfs dfs -cat /user/cloudera/problem1/result4b-csv/part-00000 | head -n 10

$ hdfs dfs -ls /user/cloudera/problem1/result4c-csv
$ hdfs dfs -cat /user/cloudera/problem1/result4c-csv/part-00000 | head -n 10
*/