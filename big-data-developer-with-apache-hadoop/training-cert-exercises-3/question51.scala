/**
 * Problem Scenario 72 : You have been given a table named "employee2" with following detail.
 * first_name string
 * last_name string
 * Write a spark script in python which read this table and print all the rows and individual column values.
 */

Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1 : Import statements for HiveContext 
$ pyspark
from pyspark.sql import HiveContext 
//Step 2 : Create sqlContext 
sqlContext = HiveContext(sc) 
//Step 3 : Query hive 
employee2 = sqlContext.sql("select * from employee2") 
//Step 4 : Now prints the data 
for row in employee2.collect(): print(row) 
//Step 5 : Print specific column 
for row in employee2.collect(): print(row.first_name)
for row in employee2.collect(): print(row.first_name + "-" + row.last_name)

//Other solution
sqlContext.sql("select * from employee2").show()
sqlContext.sql("select first_name, last_name from employee2").show()
sqlContext.sql("select first_name from employee2").show()
sqlContext.sql("select last_name from employee2").show()