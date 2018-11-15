/** Question 59
 * Problem Scenario 84 : In Continuation of previous question, please accomplish following activities.
 * 1. Select all the products which has product code as null
 * 2. Select all the products, whose name starts with Pen and results should be order by Price descending order.
 * 3. Select all the products, whose name starts with Pen and results should be order by Price descending order and quantity ascending order.
 * 4. Select top 2 products by price
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Tables are already in Hive
sqlContext.sql("show databases").show()
sqlContext.sql("use test").show()
sqlContext.sql("show tables").show()
sqlContext.sql("describe product").show()
//Step 1: Select all the products which has product code as null
sqlContext.sql("SELECT * from product WHERE product_code IS NULL").show()
sqlContext.sql("SELECT * from product").show()

//Step 2: Select all the products, whose name starts with Pen and results should be order by Price descending order.
sqlContext.sql("SELECT * FROM product WHERE name LIKE('Pen %') ORDER BY price DESC").show()

//Step 3: Select all the products, whose name starts with Pen and results should be order by Price descending order and quantity ascending order.
sqlContext.sql("SELECT * FROM product WHERE name LIKE('Pen %') ORDER BY price DESC, quantity ASC").show()

//Step 4: Select top 2 products by price
sqlContext.sql("SELECT * FROM product ORDER BY price DESC LIMIT 2").show()