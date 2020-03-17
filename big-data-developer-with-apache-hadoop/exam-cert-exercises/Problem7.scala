/**
Question 7: Correct
PreRequiste:
[Prerequisite section will not be there in actual exam]
Run below sqoop command to import few columns from customer table from mysql into hdfs to the destination /user/cloudera/practice1/problem7/customer/avro_snappy as avro file.

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table customers \
--target-dir /user/cloudera/practice1/problem7/customer/avro \
--columns "customer_id,customer_fname,customer_lname" --as-avrodatafile

Instructions:
Convert data-files stored at hdfs location /user/cloudera/practice1/problem7/customer/avro into tab delimited file using gzip compression and save in HDFS.

Output Requirement:
Result should be saved in /user/cloudera/practice1/problem7/customer_text_gzip Output file should be saved as tab delimited file in gzip Compression.

Sample Output:

21 Andrew Smith
111 Mary Jons

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table customers \
--delete-target-dir \
--target-dir /user/cloudera/practice1/problem7/customer/avro \
--columns "customer_id,customer_fname,customer_lname" \
--as-avrodatafile \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Problem7 {

  val spark = SparkSession
    .builder()
    .appName("Problem7")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "Problem7")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val rootPath = "hdfs://quickstart.cloudera/user/cloudera/practice1/problem7/customer/"

  def main(args: Array[String]): Unit = {
    try {
      Logger.getRootLogger.setLevel(Level.ERROR)

      import com.databricks.spark.avro._

      val customers = sqlContext
        .read
        .avro(s"${rootPath}avro")

      customers
        .write
        .option("sep","\t")
        .option("compression","gzip")
        .option("header",true)
        .csv(s"${rootPath}text_gzip")

      // Check the results
      // hdfs dfs -text practice1/problem7/customer/text_gzip/part* | head -n 10

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
--table customers \
--delete-target-dir \
--target-dir /user/cloudera/practice1/problem7/customer/avro \
--columns "customer_id,customer_fname,customer_lname" \
--as-avrodatafile \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

import com.databricks.spark.avro._
val customers = sqlContext.read.avro("/user/cloudera/practice1/problem7/customer/avro")
customers.show()
customers.rdd.map(r => r.mkString("\t")).saveAsTextFile("/user/cloudera/practice1/problem7/customer_text_gzip", classOf[org.apache.hadoop.io.compress.GzipCodec])

$ hdfs dfs -ls /user/cloudera/practice1/problem7/customer_text_gzip
$ hdfs dfs -text /user/cloudera/practice1/problem7/customer_text_gzip/part-00000.gz
*/