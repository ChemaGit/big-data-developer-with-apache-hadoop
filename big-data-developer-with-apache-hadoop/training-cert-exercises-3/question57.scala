/** Question 57
 * Problem Scenario 73 : You have been given data in json format as below.
 * {"first_name":"Ankit", "last_name":"Jain"}
 * {"first_name":"Amir", "last_name":"Khan"}
 * {"first_name":"Rajesh", "last_name":"Khanna"}
 * {"first_name":"Priynka", "last_name":"Chopra"}
 * {"first_name":"Kareena", "last_name":"Kapoor"}
 * {"first_name":"Lokesh", "last_name":"Yadav"}
 * Do the following activity
 * 1. create employee.json file locally.
 * 2. Load this file on hdfs
 * 3. Register this data as a temp table in Spark using Python.
 * 4. Write select query and print this data.
 * 5. Now save back this selected data in json format.
 */

// Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1 : create employee.json file locally. 
vi employee.json (press insert) past the content.
 
//Step 2 : Upload this tile to hdfs, default location 
hadoop fs -put employee.json /files/employee.json 

//Step 3 : Write spark script 
#import SQLContext 
from pyspark import SQLContext 

#Create instance of SQLContext 
sqlContext = SQLContext(sc) 

#Load json file 
employee = sqlContext.jsonFile("/files/employee.json") 

#Register RDD as a temp table 
employee.registerTempTable("EmployeeTab") 

#Select data from Employee table 
employeeInfo = sqlContext.sql("select * from EmployeeTab") 

#Iterate data and print 
for row in employeeInfo.collect(): print(row) 

//Step 4 : Write dataas a Text file 
employeeInfo.toJSON().saveAsTextFile("/files/employeeJson1") 

//Step 5: Check whether data has been created or not 
$ hadoop fs -cat /files/employeeJson1/part*

/*******BEST SOLUTION***************/

//Step 1: create employee.json file locally.
$ gedit employee.json &

//Step 2: Load this file on hdfs
$ hdfs dfs -put employee.json /files
$ hdfs dfs -cat /files/employee.json

//Step 3: Register this data as a temp table in Spark using Python.
$ pyspark
sqlContext.read.json("/files/employee.json").registerTempTable("employee")

//Step 4: Write select query and print this data
results = sqlContext.sql("SELECT * FROM employee")
results.show()

//Step 5: Now save back this selected data in json format.
results.printSchema()
results.repartition(1).write.json("/files/employee")

$ hdfs dfs -ls /files/employee
$ hdfs dfs -cat /files/employee/part-r-00000-ecf1e5e4-bf58-4994-b43a-58eecb4b8aad