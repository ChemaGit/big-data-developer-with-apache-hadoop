import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

/** Question 42
  * Problem Scenario 85 : In Continuation of previous question, please accomplish following activities.
  * 1. Select all the columns from product table with output header as below. productID AS ID code AS Code name AS Description price AS 'Unit Price'
  * 2. Select code and name both separated by '-' and header name should be 'Product Description'.
  * 3. Select all distinct prices.
  * 4. Select distinct price and name combination.
  * 5. Select all price data sorted by both code and productID combination.
  * 6. count number of products.
  * 7. Count number of products for each code.
  */

object question42 {

  val warehouseLocation = "/home/hive/warehouse"

  val spark = SparkSession
    .builder()
    .appName("question42")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question42")  // To silence Metrics warning
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir",warehouseLocation)
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/files/product.csv"
  val location = "hdfs://quickstart.cloudera/user/cloudera/tables/t_products"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try  {

      val schema = StructType(List(StructField("id", IntegerType, false), StructField("code",StringType, false),
        StructField("name", StringType, false), StructField("quantity",IntegerType, false),
        StructField("price",DoubleType, false), StructField("supplierID",IntegerType, false)))

      val products = sqlContext
        .read
        .schema(schema)
        .option("header", false)
        .option("sep",",")
        .csv(path)
        .rdd
        .map(r => r.mkString(","))
        .saveAsTextFile(location)

      sqlContext
        .sql("USE default")

      sqlContext.sql(
        s"""CREATE EXTERNAL TABLE  IF NOT EXISTS t_products(
           |id INT,
           |code STRING,
           |name STRING,
           |quantity INT,
           |price DOUBLE,
           |idSupplier INT)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
           |STORED AS TEXTFILE
           |LOCATION '$location' """.stripMargin)

      sqlContext
        .sql("show tables")
        .show()

      sqlContext
        .sql(
          """SELECT *
            |FROM t_products""".stripMargin)
        .show()


      // 1. Select all the columns from product table with output header as below. productID AS ID code AS Code name AS Description price AS 'Unit Price'
      sqlContext
        .sql(
          """SELECT id AS ID, code AS Code, name AS Description,quantity, price
            |FROM t_products""".stripMargin)
        .show()

      // 2. Select code and name both separated by '-' and header name should be 'Product Description'.
      sqlContext
        .sql(
          """SELECT concat(code,"-",name) AS `Product Description`
            |FROM t_products""".stripMargin)
        .show()

      // 3. Select all distinct prices.
      sqlContext
        .sql(
          """SELECT DISTINCT(price)
            |FROM t_products""".stripMargin)
        .show()

      // 4. Select distinct price and name combination.
      sqlContext
        .sql(
          """SELECT DISTINCT(price), name
            |FROM t_products""".stripMargin)
        .show()

      // 5. Select all price data sorted by both code and productID combination.
      sqlContext
        .sql(
          """SELECT id,code, price
            |FROM t_products SORT BY code, id""".stripMargin)
        .show()

      // 6. count number of products.
      sqlContext
        .sql(
          """SELECT COUNT(*) AS num_products
            |FROM t_products""".stripMargin)
        .show()

      // 7. Count number of products for each code.
      sqlContext
        .sql(
          """SELECT code, COUNT(*)
            |FROM t_products GROUP BY code""".stripMargin)
        .show()

      sqlContext
        .sql("""DROP TABLE t_products""")

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
// SPARK SOLUTION
val product = sc.textFile("/user/cloudera/files/product.csv").map(lines => lines.split(",")).filter(r => r(0) != "productID").map(r => (r(0).toInt,r(1),r(2),r(3).toInt,r(4).toFloat,r(5).toInt)).toDF("productID","code","name","quantity","price","idSub")

product.registerTempTable("pr")

// 1. Select all the columns from product table with output header as below. productID AS ID code AS Code name AS Description price AS 'Unit Price'
sqlContext.sql("""SELECT productID as ID,code as Code,name as Description, price as `Unit Price` FROM pr""").show()

// 2. Select code and name both separated by '-' and header name should be 'Product Description'.
sqlContext.sql("""SELECT concat(code,"-",name) AS `Product Description` FROM pr""").show()

// 3. Select all distinct prices.
sqlContext.sql("""SELECT DISTINCT(price) FROM pr""").show()

// 4. Select distinct price and name combination.
sqlContext.sql("""SELECT DISTINCT(name), price FROM pr""").show()

// 5. Select all price data sorted by both code and productID combination.
sqlContext.sql("""SELECT productID, code, price from pr ORDER BY productID, code""").show()

// 6. count number of products.
sqlContext.sql("""SELECT COUNT(productID) as `Total Products` FROM pr""").show()

// 7. Count number of products for each code.
sqlContext.sql("""SELECT code, count(productID) FROM pr GROUP BY code""").show()

//HIVE SOLUTION
$ hive
hive> use pruebas;
hive> show tables;
hive> SELECT productid as ID,productcode as Code,name as Description, price as `Unit Price` FROM t_product_parquet;
hive> SELECT concat(productcode,"-",name) AS `Product Description` FROM t_product_parquet;
hive> SELECT DISTINCT(price) FROM t_product_parquet;
hive> SELECT DISTINCT(name), price FROM t_product_parquet;
hive> SELECT productid, productcode, price from t_product_parquet ORDER BY productid, productcode;
hive> SELECT COUNT(productid) as `Total Products` FROM t_product_parquet;
hive> SELECT productcode, count(productid) FROM t_product_parquet GROUP BY productcode;
*/