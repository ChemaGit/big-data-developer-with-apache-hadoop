/**
 * Problem Scenario 87 : You have been given below three files
 * product.csv (Create this file in hdfs)
 * productID,productCode,name,quantity,price,supplierid
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
 * supplier.csv
 * supplierid,name,phone
 * 501,ABC Traders,88881111
 * 502,XYZ Company,88882222
 * 503,QQ Corp,88883333
 * products_suppliers.csv
 * productID,supplierID
 * 2001,501
 * 2002,501
 * 2003,501
 * 2004,502
 * 2001,503
 * Now accomplish all the queries given in solution.
 * Select product, its price , its supplier name where product price is less than 0.6 using SparkSQL
 */
//Explanation: Solution : 
//Step 1: 
hdfs dfs -mkdir sparksql2 
hdfs dfs -put product.csv sparksql2/ 
hdfs dfs -put supplier.csv sparksql2/ 
hdfs dfs -put products_suppliers.csv sparksql2/ 

//Step 2 : Now in spark shell 
// this Is used to Implicitly convert an RDD to a DataFrame. 
> import sqlContext.implicits._ 
// Import Spark SQL data types and Row. 
> import org.apache.spark.sql._ 
// load the data into a new RDD 
> val products = sc.textFile("sparksql2/product.csv") 
> val supplier = sc.textFile("sparksql2/supplier.csv") 
> val prdsup = sc.textFile("sparksql2/products_suppliers.csv") 
// Return the first element in this RDD 
> products.first() 
> supplier.first() 
> prdsup.first() 
//define the schema using a case class 
> case class Product(productid: Int, code: String, name: String, quantity: Int, price: Float, supplierid: Int) 
> case class Suplier(supplierid: Int, name: String, phone: String) 
> case class PRDSUP(productid: Int, supplierid: Int) 
// create an RDD of Product objects 
> val prdRDD = products.map(_.split(",")).map(p => Product(p(0).toInt,p(1),p(2),p(3).toInt,p(4).toFloat,p(5).toInt)) 
> val supRDD = supplier.map(_.split(",")).map(p => Suplier(p(0).toInt,p(1),p(2))) 
> val prdsupRDD = prdsup.map(_.split(",")).map(p => PRDSUP(p(0).toInt,p(1).toInt)) 
> prdRDD.first() 
> prdRDD.count() 
> supRDD.first() 
> supRDD.count() 
> prdsupRDD.first() 
> prdsupRDD.count()
// change RDD of Product objects to a DataFrame 
> val prdDF = prdRDD.toDF() 
> val supDF = supRDD.toDF() 
> val prdsupDF = prdsupRDD.toDF() 
// register the DataFrame as a temp table 
> prdDF.registerTempTable("products") 
> supDF.registerTempTable("suppliers") 
> prdsupDF.registerTempTable("productssuppliers") 
//Select product, its price , its supplier name where product price is less than 0.6 
> val results = sqlContext.sql("""SELECT products.name, price, suppliers.name as sup_name FROM products JOIN suppliers ON products.supplierID = suppliers.supplierID WHERE price < 0.6""") 
> results. show()

/********ANOTHER SOLUTION USING SQOOP*******************/

//First: we create the files, edit with the data and store them in HDFS
$ gedit product.csv supplier.csv products_suppliers.csv &
$ hdfs dfs -mkdir /files/product
$ hdfs dfs -put product.csv supplier.csv products_suppliers.csv /files/product

//Second: Export the data with Sqoop to mysql DB. 
//The tables have to be created in MySql before Export with Sqoop

sqoop export \
--connect jdbc:mysql://localhost/test \
--username training \
--password training \
--table product \
--export-dir /files/product/product.csv \
--outdir /home/training/Desktop/outdir \
--bindir /home/training/Desktop/bindir \
--num-mappers 1

sqoop export \
--connect jdbc:mysql://localhost/test \
--username training \
--password training \
--table supplier \
--export-dir /files/product/supplier.csv \
--outdir /home/training/Desktop/outdir \
--bindir /home/training/Desktop/bindir \
--num-mappers 1

sqoop export \
--connect jdbc:mysql://localhost/test \
--username training \
--password training \
--table products_suppliers \
--export-dir /files/product/products_suppliers.csv \
--outdir /home/training/Desktop/outdir \
--bindir /home/training/Desktop/bindir \
--num-mappers 1

//Third: Import the data from MySql to Hive with Sqoop

sqoop import \
--connect jdbc:mysql://localhost/test \
--username training \
--password training \
--table product \
--hive-import \
--hive-table test.product \
--hive-home /user/hive/warehouse \
--outdir /home/training/Desktop/outdir \
--bindir /home/training/Desktop/bindir \
--num-mappers 1

sqoop import \
--connect jdbc:mysql://localhost/test \
--username training \
--password training \
--table supplier \
--hive-import \
--hive-table test.supplier \
--hive-home /user/hive/warehouse \
--outdir /home/training/Desktop/outdir \
--bindir /home/training/Desktop/bindir \
--num-mappers 1

sqoop import \
--connect jdbc:mysql://localhost/test \
--username training \
--password training \
--table products_suppliers \
--hive-import \
--hive-table test.products_suppliers \
--hive-home /user/hive/warehouse \
--outdir /home/training/Desktop/outdir \
--bindir /home/training/Desktop/bindir \
--num-mappers 1

//Step 1: Select product, its price , its supplier name where product price is less than 0.6 using SparkSQL

$ spark-shell
> sqlContext.sql("show databases").show()
> sqlContext.sql("use test").show()
> sqlContext.sql("show tables").show()
> sqlContext.sql("SELECT product.name, product.price, supplier.name FROM product JOIN supplier ON(product.supplier_id = supplier.supplier_id) WHERE product.price < 0.6").show()
//Other query solution
> sqlContext.sql("SELECT product.name, product.price, supplier.name FROM product, supplier WHERE product.supplier_id = supplier.supplier_id and product.price < 0.6").show()