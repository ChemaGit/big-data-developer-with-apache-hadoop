/**
 * Problem Scenario 81 : You have been given MySQL DB with following details. You have
 * been given following product.csv file
 * product.csv
 * productID,productCode,name,quantity,price
 * 1001,PEN,Pen Red,5000,1.23
 * 1002,PEN,Pen Blue,8000,1.25
 * 1003,PEN,Pen Black,2000,1.25
 * 1004,PEC,Pencil 2B,10000,0.48
 * 1005,PEC,Pencil 2H,8000,0.49
 * 1006,PEC,Pencil HB,0,9999.99
 * Now accomplish following activities.
 * 1. Create a Hive ORC table using SparkSql
 * 2. Load this data in Hive table.
 * 3. Create a Hive parquet table using SparkSQL and load data in it.
 *
 * Answer : See the explanation for Step by Step Solution and configuration.
 *
 *Explanation: Solution : 
 *Step 1 : 
 *  Create this file in HDFS under following directory (Without header} /user/cloudera/he/exam/task1/product.csv 
 *Step 2 :
 *  Now using Spark-shell read the file as RDD // load the data into a new RDD 
 *  val products = sc.textFile("/user/cloudera/he/exam/task1/product.csv") // Return the first element in this RDD 
 *  products.first() 
 *Step 3 : 
 *  Now define the schema using a case class 
 *  case class Product(productid: Integer, code: String, name: String, quantity:lnteger, price: Float) 
 *Step 4 : 
 *  create an RDD of Product objects 
 *  val prdRDD = products.map(_.split(",")).map(p => Product(p(0).tolnt,p(1),p(2),p(3}.tolnt,p(4}.toFloat)) prdRDD.first() 
 * prdRDD.count() 
 *Step 5 : 
 *  Now create data frame 
 *  val prdDF = prdRDD.toDF() 
 *Step 6 : 
 *  Now store data in hive warehouse directory. (However, table will not be created } 
 *  import org.apache.spark.sql.SaveMode 
 *  prdDF.write.mode(SaveMode.Overwrite).format("orc").saveAsTable("product_orc_table") 
 *step 7: 
 *  Now create table using data stored in warehouse directory. 
 * With the help of hive. hive show tables 
 * CREATE EXTERNAL TABLE products (productid Int, code String, name String, quantity Int, price Float) STORED AS orc LOCATION "/user/hive/warehouse/product_orc_table"; 
 *Step 8 : 
 *  Now create a parquet table 
 * import org.apache.spark.sql.SaveMode 
 * prdDF.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("product_parquet_ table") 
 *Step 9 : 
 *  Now create table using this 
 * CREATE EXTERNAL TABLE products_parquet(productid Int, code String, name String, quantity Int, price Float) STORED AS parquet LOCATION "/user/hive/warehouse/product_parquet_table"; 
 *Step 10 : Check data has been loaded or not. Select * from products; Select * from products_parquet;
 */
  //step 1
  $ hdfs dfs -mkdir /user/exam  
  $ hdfs dfs -put /home/training/Desktop/files/product.csv /user/exam
  //step 2
  val products = sc.textFile("/user/exam/product.csv")
  products.first
  //step 3
  case class Product(productid: Integer, code: String, name: String, quantity:Integer, price: Float)
  //step 4
  val prdRDD = products.map(line => line.split(",")).map(arr => new Product(arr(0).toInt,arr(1),arr(2),arr(3).toInt,arr(4).toFloat))
  prdRDD.count()
  //step 5
  val prdDF = prdRDD.toDF()
  //step 6
  import org.apache.spark.sql.SaveMode
  prdDF.write.mode(SaveMode.Overwrite).format("orc").saveAsTable("product_orc_table")
  //step 7
  $ hive
  > show tables;
  > CREATE EXTERNAL TABLE products(productid Int, code String, name String, quantity Int, price Float) STORED AS orc LOCATION "/user/hive/warehouse/product_orc_table";
  > show tables;
  //step 8
  prdDF.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("product_parquet_table")
  //step 9
  > CREATE EXTERNAL TABLE products_parquet(productid Int, code String, name String, quantity Int, price Float) STORED AS parquet LOCATION "/user/hive/warehouse/product_parquet_table";
  //step 10
  > select * from products_parquet;
  
  /************A BETTER WAY TO TO THIS************************/
case class Product(id: Int, code: String, name: String, quantity: Int, price: Float)

val p = sc.textFile("/files/product.csv").map(lines => lines.split(','))
val first = p.first
val product = p.filter(arr => arr(0) != first(0)).map(arr => Product(arr(0).toInt, arr(1), arr(2), arr(3).toInt, arr(4).toFloat)).toDF
product.registerTempTable("product_mine")
sqlContext.sql("CREATE TABLE retail_cca174.product_orc STORED AS ORC AS SELECT * from product_mine")
sqlContext.sql("CREATE TABLE retail_cca174.product_parquet STORED AS PARQUET AS SELECT * from product_mine")
sqlContext.sql("select * from product_orc").show
sqlContext.sql("select * from product_parquet").show