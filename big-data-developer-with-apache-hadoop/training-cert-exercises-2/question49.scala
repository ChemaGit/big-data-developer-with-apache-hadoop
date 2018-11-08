/**
 * Problem Scenario 88 : You have been given below three files
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
 * 1. It is possible that, same product can be supplied by multiple supplier. Now find each product, its price according to each supplier.
 * 2. Find all the supllier name, who are supplying 'Pencil 3B'
 * 3. Find all the products , which are supplied by ABC Traders.
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 

$ gedit product.csv supplier.csv products_suppliers.csv &
$ cd /home/training/Desktop/files
$ hdfs dfs -mkdir /files/product
$ hdfs dfs -put product.csv supplier.csv products_suppliers.csv /files/product
$ hdfs dfs -ls /files/product

//Step 1 : It is possible that, same product can be supplied by multiple supplier. Now find each product, its price according to each supplier. 
case class Product(id: Int, code: String, name: String, quantity: Int,price: Double, supplierId: Int)
case class Supplier(id: Int, name: String, phone: String)
case class ProductSupplier(productId: Int, supplierId: Int)

val product = sc.textFile("/files/product/product.csv").map(line => line.split(",")).map(arr => new Product(arr(0).toInt,arr(1),arr(2),arr(3).toInt,arr(4).toDouble,arr(5).toInt)).toDF
val supp = sc.textFile("/files/product/supplier.csv").map(line => line.split(",")).map(arr => new Supplier(arr(0).toInt,arr(1),arr(2))).toDF
val proSupp = sc.textFile("/files/product/products_suppliers.csv").map(line => line.split(",")).map(arr => new ProductSupplier(arr(0).toInt,arr(1).toInt)).toDF

product.registerTempTable("product")
supp.registerTempTable("supplier")
proSupp.registerTempTable("prod_supp")

val results = sqlContext.sql("SELECT product.name AS ProductName, price, supplier.name AS SupplierName FROM prod_supp JOIN product ON (prod_supp.productId = product.id) JOIN supplier ON (prod_supp.supplierId = supplier.id)")
results.registerTempTable("joined")
results.show() 

//Step 2 : Find all the supllier name, who are supplying 'Pencil 3B' 
val step2 = sqlContext.sql("SELECT ProductName, SupplierName FROM joined WHERE ProductName = 'Pencil 3B'")
step2.show()
//Other solution
val results = sqlContext.sql("SELECT p.name AS ProductName, s.name AS SupplierName FROM prod_supp AS ps JOIN product AS p ON ps.productId = p.id JOIN supplier AS s ON ps.supplierId = s.id WHERE p.name = 'Pencil 3B'") 
results.show() 

//Step 3 : Find all the products , which are supplied by ABC Traders. 
val step3 = sqlContext.sql("SELECT ProductName, SupplierName FROM joined WHERE SupplierName = 'ABC Traders'")
step3.show()
//Other solution
val results = sqlContext.sql("SELECT p.name AS ProductName, s.name AS SupplierName FROM product AS p, prod_supp AS ps, supplier AS s WHERE p.id = ps.productId AND ps.supplierId = s.id AND s.name = 'ABC Traders'") 
results. show()