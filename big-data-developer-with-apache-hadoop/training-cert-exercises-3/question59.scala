/** Question 59
  * Problem Scenario 84 : In Continuation of previous question, please accomplish following activities.
  * 1. Select all the products which has product code as null
  * 2. Select all the products, whose name starts with Pen and results should be order by Price descending order.
  * 3. Select all the products, whose name starts with Pen and results should be order by Price descending order and quantity ascending order.
  * 4. Select top 2 products by price
  */
sqlContext.sql("show databases").show()
sqlContext.sql("use pruebas")
sqlContext.sql("show tables").show()
sqlContext.sql("describe t_product_parquet").show()
//+-----------+---------+-------+
//|   col_name|data_type|comment|
//+-----------+---------+-------+
//|  productID|      int|       |
//|productCode|   string|       |
//|       name|   string|       |
//|   quantity|      int|       |
//|      price|    float|       |
//+-----------+---------+-------+
sqlContext.sql("""select * from t_product_parquet where productCode is null""").show()
sqlContext.sql("""select * from t_product_parquet where name like("Pen%") order by price desc""").show()
sqlContext.sql("""select * from t_product_parquet where name like("Pen%") order by price desc, quantity""").show()
sqlContext.sql("""select * from t_product_parquet order by price desc limit 2""").show()