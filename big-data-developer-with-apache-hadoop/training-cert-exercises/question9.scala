/**
 * Problem Scenario 89 : You have been given below patient data in csv format,
 * patientID,name,dateOfBirth,lastVisitDate
 * 1001,Ah Teck,1991-12-31,2012-01-20
 * 1002,Kumar,2011-10-29,2012-09-20
 * 1003,Ali,2011-01-30,2012-10-21
 * Accomplish following activities.
 * 1. Find all the patients whose lastVisitDate between current time and '2012-09-15'
 * 2. Find all the patients who born in 2011
 * 3. Find all the patients age
 * 4. List patients whose last visited more than 60 days ago
 * 5. Select patients 18 years old or younger
 */
$ hdfs dfs -put /home/training/Desktop/files/patients.csv /files/
$ hdfs dfs -ls /files/patients.csv
$ hdfs dfs -cat /files/patients.csv

import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
import sqlContext.implicits._
import org.apache.spark.sql._

val rdd = sc.textFile("/files/patients.csv").map(line => line.split(","))
case class Patient(patienID: Int,name: String,dateBirth: String, lastVisit: String)
rdd.map(arr => new Patient(arr(0).toInt, arr(1), arr(2), arr(3) ) ).toDF().registerTempTable("patient")
val result = sqlContext.sql("SELECT * FROM patient")
result.show()
//1. Find all the patients whose lastVisitDate between current time and '2012-09-15'
val results = sqlContext.sql("SELECT * FROM patient WHERE TO_DATE(CAST(UNIX_TIMESTAMP(lastVisit, 'yyyy-MM-dd') AS TIMESTAMP)) BETWEEN '2012-09-15' AND current_timestamp() ORDER BY lastVisit") 
val results = sqlContext.sql("SELECT * FROM patient WHERE unix_timestamp(lastVisit,'yyyy-MM-dd') >= unix_timestamp('2012-09-15','yyyy-MM-dd')")
results.show()
//2. Find all the patients who born in 2011
val results = sqlContext.sql("SELECT * FROM patient WHERE year(dateBirth) = 2011")
results.show()
val results = sqlContext.sql("SELECT * FROM patient WHERE YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(dateBirth, 'yyyy-MM-dd') AS TIMESTAMP))) = 2011") 
results. show() 
//3. Find all the patients age
val results = sqlContext.sql("SELECT name, dateBirth, datediff(current_date(), TO_DATE(CAST(UNIX_TIMESTAMP(dateBirth, 'yyyy-MM-dd') AS TIMESTAMP)))/365 AS age FROM patient")
results.show()
val results = sqlContext.sql("SELECT name, dateBirth, datediff('2018-10-18', dateBirth)/365 AS age FROM patient")
results.show()
//4. List patients whose last visited more than 60 days ago
val results = sqlContext.sql("SELECT * FROM patient WHERE datediff('2018-10-18', lastVisit) > 60")
results.show()
val results = sqlContext.sql("SELECT name, lastVisit FROM patient WHERE datediff(current_date(), TO_DATE(CAST(UNIX_TIMESTAMP(lastVisit, 'yyyy-MM-dd') AS TIMESTAMP))) > 60"); 
results.show(); 
//5. Select patients 18 years old or younger
val results = sqlContext.sql("SELECT *,  datediff('2018-10-18', dateBirth)/365 AS age FROM patient WHERE datediff('2018-10-18', dateBirth)/365 <= 18")
results.show()
val results = sqlContext.sql("SELECT * FROM patient WHERE (datediff(current_date(), TO_DATE(CAST(UNIX_TIMESTAMP(dateBirth, 'yyyy-MM-dd') AS TIMESTAMP)))/365) <= 18")
results.show()