/** Question 91
 * Problem Scenario 82 : You have been given table in Hive with following structure (Which you have created in previous exercise).
 * productid int code string name string quantity int price float
 * Using SparkSQL accomplish following activities.
 * 1. Select all the products name and quantity having quantity <= 2000
 * 2. Select name and price of the product having code as 'PEN'
 * 3. Select all the products, which name starts with PENCIL
 * 4. Select all products which "name" begins with 'P\ followed by any two characters, followed by space, followed by zero or more characters
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1 : Copy following file (Mandatory Step in Cloudera QuickVM) if you have not done it. 
$ sudo su root cp /usr/lib/hive/conf/hive-site.xml /usr/lib/spark_conf/ 

//Step 2 : Now start spark-shell 
$ spark-shell
case class Product(productId: Int, code: String, name: String, quantity: Int, price: Float, supplierId: Int)
val products = sc.textFile("/files/p90_products/*").map(line => line.split(",")).map(arr => Product(arr(0).toInt,arr(1),arr(2),arr(3).toInt,arr(4).toFloat,arr(5).toInt)).toDF("product_id","code","name","quantity","price","supplier_id")
products.registerTempTable("products")

//Step 3 ; Select all the products name and quantity having quantity <= 2000 
val results = sqlContext.sql("SELECT name, quantity FROM products WHERE quantity <= 2000") 
results.show() 

//Step 4 : Select name and price of the product having code as 'PEN' 
val results = sqlContext.sql("SELECT name, price FROM products WHERE code = 'PEN'") 
results.show() 

//Step 5 : Select all the products , which name starts with Pencil 
val results = sqlContext.sql("SELECT name, price FROM products WHERE upper(name) LIKE 'Pencil%'")
results.show() 

//Step 6 : select all products which "name" begins with 'P', followed by any two characters, followed by space, followed by zero or more characters 
val results = sqlContext.sql("SELECT name, price FROM products WHERE name LIKE 'P__ %'") 
results.show()

/**************SOLUTION IN PYTHON************************/
$ pyspark
> from pyspark.sql.types import *
> fields = [StructField("id", IntegerType(), True), StructField("id_order", IntegerType(), True), StructField("id_product", IntegerType(), True), StructField("quantity", IntegerType(), True), StructField("subtotal", FloatType(), True), StructField("price", FloatType(), True)]
> schema = StructType(fields)
> order_items = sc.textFile("/files/p90_order_items/*").map(lambda lines: lines.split(",")).map(lambda arr: ( int(arr[0]), int(arr[0]), int(arr[0]), int(arr[0]), float(arr[0]), float(arr[0]) ) ).toDF(schema)
> fields = [StructField("id", IntegerType(), True),StructField("code", StringType(), True),StructField("name", StringType(), True),StructField("quantity", IntegerType(), True),StructField("price", FloatType(), True),StructField("supplier_id", IntegerType(), True) ]
> schema = StructType(fields)
> products = sc.textFile("/files/p90_products/*").map(lambda lines: lines.split(",")).map(lambda arr: ( int(arr[0]), arr[1], arr[2], int(arr[3]), float(arr[4]), int(arr[5]) ) ).toDF(schema)
> order_items.registerTempTable("order_items")
> products.registerTempTable("products")
> sqlContext.sql("select products.name, products.quantity from products where quantity <= 2000").show()
> sqlContext.sql("select name, price from products where code = 'PEN'").show()
> sqlContext.sql("select * from products where name  LIKE('Pencil%')").show()
> sqlContext.sql("select * from products where name  LIKE('P__ %')").show()