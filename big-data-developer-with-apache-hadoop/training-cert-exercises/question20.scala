/**
 * Problem Scenario 83 : In Continuation of previous question, please accomplish following activities.
 * 1. Select all the records with quantity >= 5000 and name starts with 'Pen'
 * 2. Select all the records with quantity >= 5000, price is less than 1.24 and name starts with 'Pen'
 * 3. Select all the records witch does not have quantity >= 5000 and name does not starts with 'Pen'
 * 4. Select all the products which name is 'Pen Red', 'Pen Black'
 * 5. Select all the products which has price BETWEEN 1.0 AND 2.0 AND quantity BETWEEN 1000 AND 2000.
 */
//Solution in HIVE
$ hive
//1. Select all the records with quantity >= 5000 and name starts with 'Pen' 
> Select * from products where quantity >= 5000 and name like('Pen %');
//2. Select all the records with quantity >= 5000, price is less than 1.24 and name starts with 'Pen'
> Select * from products where quantity >= 5000 and name like('Pen %') and price < 1.24;
//3. Select all the records witch does not have quantity >= 5000 and name does not starts with 'Pen'
> Select * from products where NOT(quantity >= 5000 and name like('Pen %'));
//4. Select all the products which name is 'Pen Red', 'Pen Black'
> Select * from products where name in('Pen Red', 'Pen Black');
//5. Select all the products which has price BETWEEN 1.0 AND 2.0 AND quantity BETWEEN 1000 AND 2000.
> Select * from products where price between 1.0 and 2.0 and quantity between 1000 and 2000;

//Solution in Spark
import org.apache.spark.sql.hive.HiveContext
val sqlContext = new HiveContext(sc)
//Step 1 : Select all the records with quantity >= 5000 and name starts with 'Pen' 
val results = sqlContext.sql("SELECT * FROM products WHERE quantity >= 5000 AND name LIKE 'Pen %'") 
results.show() 
//Step 2 : Select all the records with quantity >= 5000 , price is less than 1.24 and name starts with 'Pen' 
val results = sqlContext.sql("SELECT * FROM products WHERE quantity >= 5000 AND price < 1.24 AND name LIKE 'Pen %'") 
results.show()
//Step 3 : Select all the records witch does not have quantity >= 5000 and name does not starts with 'Pen' 
val results = sqlContext.sql("SELECT * FROM products WHERE NOT (quantity >= 5000 AND name LIKE 'Pen %')") 
results.show()
//Step 4 : Select all the products wchich name is 'Pen Red', 'Pen Black' 
val results = sqlContext.sql("SELECT * FROM products WHERE name IN ('Pen Red', 'Pen Black')") 
results.show()
//Step 5 : Select all the products which has price BETWEEN 1.0 AND 2.0 AND quantity BETWEEN 1000 AND 2000. 
val results = sqlContext.sql("SELECT * FROM products WHERE (price BETWEEN 1.0 AND 2.0) AND (quantity BETWEEN 1000 AND 2000)") 
results.show()