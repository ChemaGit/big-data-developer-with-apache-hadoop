package spark.spark-sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._

object WorkingWithDataframes {

  val spark = SparkSession
    .builder()
    .appName("WorkingWithDataframes")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "WorkingWithDataframes")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val input = "hdfs://quickstart.cloudera/public/cognitive_class/trips"

  val schema = StructType(List(StructField("Trip ID",IntegerType, false), StructField("Duration",IntegerType,true),StructField("Start Date",TimestampType,true),
    StructField("Start Station",StringType,true), StructField("Start Terminal",IntegerType,true), StructField("End Date",TimestampType,true),
    StructField("End Station",StringType,true), StructField("End Terminal",IntegerType,true), StructField("Bike",IntegerType,true),
    StructField("Subscription Type",StringType,true), StructField("Zip Code",StringType,true)))

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      import spark.implicits._

      // Creating Spark DataFrames
      val data = sqlContext
        .read
        .option("header",false)
        .option("sep",",")
        .option("timestampFormat","MM/dd/YYYY HH:mm")
        .option("mode","DROPMALFORMED")
        .schema(schema)
        .csv(input)
      data.printSchema()
      println(data.head())

      // Displays the content of the DataFrame
      data.show(5)

      // Selecting columns
      data
        .select("Start Station")
        .show(5)

      // Filter the DataFrame to only retain rows with duration less than 70
      data
        .filter($"Duration" < 18)
        .show(5)

      // Operating on Columns
      data
        .withColumn("Duration in Hours", $"Duration" / 60  )
        .show(6)

      // Grouping, Aggregation
      // Spark DataFrames support a number of commonly used functions to aggregate data after grouping.
      data.groupBy(col("Start Terminal"))
        .agg(round(avg("Duration"),2).as("Avg Duration Trip"))
        .show(5)

      // We can also sort the output from the aggregation
      data.groupBy(col("Start Terminal"))
        .agg(round(avg("Duration"),2).as("Avg Duration Trip"))
        .orderBy(col("Avg Duration Trip").desc)
        .show(5)

      // Running SQL Queries from Spark DataFrames
      // A Spark DataFrame can also be registered as a temporary table in Spark SQL and registering
      // a DataFrame as a table allows you to run SQL queries over its data.
      // The sql function enables applications to run SQL queries programmatically and returns the result as a DataFrame.
      data.createOrReplaceTempView("trips")

      val durationOverSixty =sqlContext
        .sql(
          """SELECT *
            |FROM trips
            |WHERE Duration > 60
          """.stripMargin)

      durationOverSixty.show(10)

      // To have the opportunity to view the web console of Spark: http://localhost:4041/
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