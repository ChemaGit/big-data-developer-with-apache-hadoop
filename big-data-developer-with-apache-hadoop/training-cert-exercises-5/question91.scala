/** Question 91
  * Problem Scenario 82 : You have been given table in Hive(pruebas.t_product_parquet) with following structure (Which you have created in previous exercise).
productid           	int
productcode         	string
name                	string
quantity            	int
price               	float
  * productid int code string name string quantity int price float
  * Using SparkSQL accomplish following activities.
  * 1. Select all the products name and quantity having quantity <= 2000
  * 2. Select name and price of the product having code as 'PEN'
  * 3. Select all the products, which name starts with Pencil
  * 4. Select all products which "name" begins with 'P' followed by any two characters, followed by space, followed by zero or more characters
  */
sqlContext.sql("show databases").show()
sqlContext.sql("use pruebas")
sqlContext.sql("show tables").show()
sqlContext.sql("select * from t_product_parquet").show()
// 1. Select all the products name and quantity having quantity <= 2000
sqlContext.sql("""SELECT name, quantity FROM t_product_parquet WHERE quantity <= 2000""").show()
// 2. Select name and price of the product having code as 'PEN'
sqlContext.sql("""SELECT productcode, name, price FROM t_product_parquet WHERE productcode = "PEN" """).show()
// 3. Select all the products, which name starts with Pencil
sqlContext.sql("""SELECT * FROM t_product_parquet WHERE name LIKE("Pencil%") """).show()
// 4. Select all products which "name" begins with 'P' followed by any two characters, followed by space, followed by zero or more characters
sqlContext.sql("""SELECT * FROM t_product_parquet WHERE name LIKE("P__ %") """).show()