import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object question54 {
  /**
    * Building files
    * $ gedit /home/cloudera/files/product.csv &
    * $ gedit /home/cloudera/files/supplier.csv &
    * $ gedit /home/cloudera/files/products_suppliers.csv &
    * $ hdfs dfs -put /home/cloudera/files/product.csv /user/cloudera/files
    * $ hdfs dfs -put /home/cloudera/files/supplier.csv /user/cloudera/files
    * $ hdfs dfs -put /home/cloudera7files/products_suppliers.csv /user/cloudera/files
    */

  val spark = SparkSession
    .builder()
    .appName("question54")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question54")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/files/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val productSchema = StructType(List(StructField("pId", IntegerType, false), StructField("code",StringType, false),
        StructField("name", StringType, false), StructField("quantity",IntegerType, false),
        StructField("price",DoubleType, false), StructField("sId",IntegerType, false)))

      val supplierSchema = StructType(List(StructField("supId", IntegerType, false), StructField("name",StringType, false),
        StructField("phone", StringType, false)))

      val productSupplierSchema = StructType(List(StructField("prId", IntegerType, false), StructField("sppId",IntegerType, false)))

      val products = sqlContext
        .read
        .schema(productSchema)
        .option("header", false)
        .option("sep",",")
        .csv(s"${path}product.csv")
        .cache()

      val supplier = sqlContext
        .read
        .schema(supplierSchema)
        .option("header", false)
        .option("sep",",")
        .csv(s"${path}supplier.csv")
        .cache()

      val pr_sp = sqlContext
        .read
        .schema(productSupplierSchema)
        .option("header", false)
        .option("sep",",")
        .csv(s"${path}products_suppliers.csv")
        .cache()

      products.createOrReplaceTempView("pr")
      supplier.createOrReplaceTempView("sp")
      pr_sp.createOrReplaceTempView("pr_sp")

      sqlContext
        .sql(
          """SELECT code, name, price, sName, phone
            |FROM pr JOIN pr_sp ON(pId = prId) JOIN sp ON(spId = sId)
            |WHERE price < 0.6""".stripMargin)
        .show()

      products.unpersist()
      supplier.unpersist()
      pr_sp.unpersist()

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
$ gedit /home/cloudera/files/product.csv &
$ gedit /home/cloudera/files/supplier.csv &
$ gedit /home/cloudera/files/products_suppliers.csv &
$ hdfs dfs -put /home/cloudera/files/product.csv /user/cloudera/files
$ hdfs dfs -put /home/cloudera/files/supplier.csv /user/cloudera/files
$ hdfs dfs -put /home/cloudera/files/products_suppliers.csv /user/cloudera/files

val product = sc.textFile("/user/cloudera/files/product.csv").map(line => line.split(",")).filter(r => r(0) != "productID").map(r => (r(0).toInt,r(1),r(2),r(3).toInt,r(4).toFloat)).toDF("pId","code","name","quantity","price")
val supplier = sc.textFile("/user/cloudera/files/supplier.csv").map(line => line.split(",")).map(r => (r(0).toInt,r(1),r(2))).toDF("sId","sName","phone")
val pr_sp = sc.textFile("/user/cloudera/files/products_suppliers.csv").map(line => line.split(",")).map(r => (r(0).toInt,r(1).toInt)).toDF("prId","spId")

product.registerTempTable("pr")
supplier.registerTempTable("sp")
pr_sp.registerTempTable("pr_sp")

sqlContext.sql("""select code, name,price,sName, phone from pr join pr_sp on(pId = prId) join sp on(spId = sId) where price < 0.6""").show()
*/