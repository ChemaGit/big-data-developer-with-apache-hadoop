/** Question 52
  * hive table t_product_parquet
  * id   code    name          quantity  price
  * 1001	PEN	Pen Red	        5000	1.23
  * 1002	PEN	Pen Blue	8000	1.25
  * 1003	PEN	Pen Black	2000	1.25
  * 1004	PEC	Pencil 2B	10000	0.48
  * 1005	PEC	Pencil 2H	8000	0.49
  * 1006	PEC	Pencil HB	0	9999.99
  * 2001	PEC	Pencil 3B	500	0.52
  * 2002	PEC	Pencil 4B	200	0.62
  * 2003	PEC	Pencil 5B	100	0.73
  * 2004	PEC	Pencil 6B	500	0.47
  * Problem Scenario 86 : In Continuation of previous question, please accomplish following activities.
  * 1. Select Maximum, minimum, average , Standard Deviation for price column, and total quantity.
  * 2. Select minimum and maximum price for each product code.
  * 3. Select Maximum, minimum, average , Standard Deviation for price column, and total quantity for each product code, however make sure Average and Standard deviation will have maximum two decimal values.
  * 4. Select all the product code and average price only where product count is more than or equal to 3.
  * 5. Select maximum, minimum , average price and total quantity of all the products for each code. Also produce the same across all the products.
  */
sqlContext.sql("use pruebas")
sqlContext.sql("describe t_product_parquet").show()
sqlContext.sql("""select max(price) as max_price, min(price) as min_price, round(avg(price),2) as avg_price,round(stddev(price),2) as std_dev_price, sum(quantity) as total_quantity from t_product_parquet""").show()
sqlContext.sql("""select productCode, min(price) as min_price, max(price) as max_price from t_product_parquet group by productCode""").show()
sqlContext.sql("""select productCode,max(price) as max_price, min(price) as min_price, round(avg(price),2) as avg_price,round(stddev(price),2) as std_dev_price, sum(quantity) as total_quantity from t_product_parquet group by productCode """).show()
sqlContext.sql("""select productCode, round(avg(price),2) as avg_price from t_product_parquet group by productCode having count(productID) >= 3""").show()
sqlContext.sql("""select productCode,max(price) as max_price, min(price) as min_price, round(avg(price),2) as avg_price, sum(quantity) as total_quantity from t_product_parquet group by productCode with rollup""").show()