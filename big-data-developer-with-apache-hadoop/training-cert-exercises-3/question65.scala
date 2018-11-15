/** Question 65
 * Problem Scenario 91 : You have been given data in json format as below.
 * {"first_name":"Ankit", "last_name":"Jain"}
 * {"first_name":"Amir", "last_name":"Khan"}
 * {"first_name":"Rajesh", "last_name":"Khanna"}
 * {"first_name":"Priynka", "last_name":"Chopra"}
 * {"first_name":"Kareena", "last_name":"Kapoor"}
 * {"first_name":"Lokesh", "last_name":"Yadav"}
 * Do the following activity
 * 1. create employee.json tile locally.
 * 2. Load this tile on hdfs
 * 3. Register this data as a temp table in Spark using Python.
 * 4. Write select query and print this data.
 * 5. Now save back this selected data in json format.
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1 : create employee.json file locally. 
$ vi employee.json (press insert) past the content. 

//Step 2 : Upload this file to hdfs, default location 
hadoop fs -put employee.json 
val employee = sqlContext.read.json("/files/employee.json") 
employee.repartition(1).write.parquet("/files/spark66/employee.parquet") 
val parq_data = sqlContext.read.parquet("/files/spark66/employee.parquet") 
parq_data.registerTempTable("employee") 
val allemployee = sqlContext.sql("SELECT * FROM employee") 
allemployee.show() 
import org.apache.spark.sql.SaveMode 
parq_data.repartition(1).write.format("orc").saveAsTable("test.employee_orc_table") 
//Change the codec. 
sqlContext.setConf("spark.sql.parquet.compression.codec","snappy") 
employee.repartition(1).write.mode(SaveMode.Overwrite).parquet("/files/spark66/employee.parquet")

/***OTHER SOLUTION****/
//Step 1: Create the file locally and send it to HDFS
$ gedit employee.json &
$ hdfs dfs -put employee.json /files/

//Step 2: Read the file with Python
$ pyspark
> sqlContext.read.json("/files/employee.json").registerTempTable("employee")
> query = sqlContext.sql("SELECT * FROM employee")
> query.show()
> query.repartition(1).write.json("/files/spark66")