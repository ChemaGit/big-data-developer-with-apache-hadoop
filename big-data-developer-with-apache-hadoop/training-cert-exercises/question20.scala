/** Question 20
  * Problem Scenario 83 : In Continuation of previous question, please accomplish following activities.
  * 1. Select all the records with quantity >= 5000 and name starts with 'Pen'
  * 2. Select all the records with quantity >= 5000, price is less than 1.24 and name starts with 'Pen'
  * 3. Select all the records witch does not have quantity >= 5000 and name does not starts with 'Pen'
  * 4. Select all the products which name is 'Pen Red', 'Pen Black'
  * 5. Select all the products which has price BETWEEN 1.0 AND 2.0 AND quantity BETWEEN 1000 AND 2000.
  *
  * product.csv
  * productID,productCode,name,quantity,price,supplierID
  * 1001,PEN,Pen Red,5000,1.23,501
  * 1002,PEN,Pen Blue,8000,1.25,501
  * 1003,PEN,Pen Black,2000,1.25,501
  * 1004,PEC,Pencil 2B,10000,0.48,502
  * 1005,PEC,Pencil 2H,8000,0.49,502
  * 1006,PEC,Pencil HB,0,9999.99,502
  * 2001,PEC,Pencil 3B,500,0.52,501
  * 2002,PEC,Pencil 4B,200,0.62,501
  * 2003,PEC,Pencil 5B,100,0.73,501
  * 2004,PEC,Pencil 6B,500,0.47,502
  */

// Previous steps
// $ hdfs dfs -put /home/cloudera/files/product.csv /user/cloudera/files/

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object question20 {

  val spark = SparkSession
    .builder()
    .appName("question20")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question20")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/files/product.csv"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val schema = StructType(List(StructField("id", IntegerType, false), StructField("code",StringType, false),
        StructField("name", StringType, false), StructField("quantity",IntegerType, false),
        StructField("price",DoubleType, false), StructField("supplierID",IntegerType, false)))

      val products = sqlContext
        .read
        .schema(schema)
        .option("header", false)
        .option("sep",",")
        .csv(path)
        .cache()

      products.createOrReplaceTempView("products")

      sqlContext
        .sql("""SELECT * FROM products""")
        .show()

      // 1. Select all the records with quantity >= 5000 and name starts with 'Pen'
      sqlContext
        .sql(
          """SELECT *
            |FROM products
            |WHERE quantity >= 5000 and name LIKE('Pen%')""".stripMargin)
        .show()

      // 2. Select all the records with quantity >= 5000, price is less than 1.24 and name starts with 'Pen'
      sqlContext
        .sql(
          """SELECT *
            |FROM products
            |WHERE quantity >= 5000 and price < 1.24 and name LIKE('Pen%')""".stripMargin)
        .show()

      // 3. Select all the records witch does not have quantity >= 5000 and name does not starts with 'Pen'
      sqlContext
        .sql(
          """SELECT *
            |FROM products
            |WHERE NOT (quantity >= 5000 and name LIKE('Pen%'))""".stripMargin)
        .show()

      // 4. Select all the products which name is 'Pen Red', 'Pen Black'
      sqlContext
        .sql(
          """SELECT *
            |FROM products
            |WHERE name IN("Pen Red","Pen Black")""".stripMargin)
        .show()

      // 5. Select all the products which has price BETWEEN 1.0 AND 2.0 AND quantity BETWEEN 1000 AND 2000.
      sqlContext
        .sql(
          """SELECT *
            |FROM products
            |WHERE price BETWEEN 1.0 and 2.0 AND quantity BETWEEN 1000 AND 2000""".stripMargin)
        .show()

      products.unpersist()

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
val product = sc
  .textFile("/user/cloudera/files/product.csv")
  .map(line => line.split(",")).filter(r => r(0) != "productID")
  .map(r => (r(0).toInt,r(1),r(2),r(3).toInt,r(4).toFloat,r(5).toInt))
  .toDF("id","code","name","quantity","price","idSub")
product.show()
product.registerTempTable("product")

sqlContext.sql("""SELECT * FROM product WHERE quantity >= 5000 and name LIKE("Pen %")""").show()
sqlContext.sql("""SELECT * FROM product WHERE quantity >= 5000 and name LIKE("Pen %") and price < 1.24""").show()
sqlContext.sql("""SELECT * FROM product WHERE !(quantity >= 5000 and name LIKE("Pen %"))""").show()
sqlContext.sql("""SELECT * FROM product WHERE name in("Pen Red", "Pen Black")""").show()
sqlContext.sql("""SELECT * FROM product WHERE price BETWEEN 1.0 AND 2.0 AND quantity BETWEEN 1000 AND 2000""").show()
*/