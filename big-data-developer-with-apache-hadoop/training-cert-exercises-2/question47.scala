/**
 * Problem Scenario 42 : You have been given a file (spark1O/sales.txt), with the content as
 * given in below.
 * spark10/sales.txt
 * Department,Designation,costToCompany,State
 * Sales,Trainee,12000,UP
 * Sales,Lead,32000,AP
 * Sales,Lead,32000,LA
 * Sales,Lead,32000,TN
 * Sales,Lead,32000,AP
 * Sales,Lead,32000,TN
 * Sales,Lead,32000,LA
 * Sales,Lead,32000,LA
 * Marketing,Associate,18000,TN
 * Marketing,Associate,18000,TN
 * HR,Manager,58000,TN
 * And want to produce the output as a csv with group by Department,Designation,State with additional columns with sum(costToCompany) and TotalEmployeeCountt
 * Should get result like
 * Dept,Desg,state,empCount,totalCost
 * Sales,Lead,AP,2,64000
 * Sales.Lead.LA,3,96000
 * Sales,Lead,TN,2,64000
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//step 1 : Create a file first using Hue in hdfs.
$ gedit sales.txt &
$ hdfs dfs -put /home/cloudera/files/sales.txt /files/
$ hdfs dfs -cat /files/sales.txt 
//Step 2 : Load tile as an RDD 
val rawlines = sc.textFile("spark10/sales.txt") 
//Step 3 : Create a case class, which can represent its column fileds. 
case class Employee(dep: String, des: String, cost: Double, state: String) 
//Step 4 : Split the data and create RDD of all Employee objects. 
val employees = rawlines.map(_.split(",")).map(row=>Employee(row(0), row(1), row(2).toDouble, row(3))) 
//Step 5 : Create a row as we needed. All group by fields as a key and value as a count for each employee as well as its cost, 
val keyVals = employees.map( em => ((em.dep, em.des, em.state), (1 , em.cost))) 
//Step 6 : Group by all the records using reduceByKey method as we want summation as well. For number of employees and their total cost, 
val results = keyVals.reduceByKey{ (a,b) => (a._1 + b._1, a._2 + b._2)} // (a.count + b.count, a.cost + b.cost)} 
//Step 7 : Save the results in a text file as below. 
results.repartition(1).saveAsTextFile("spark10/group.txt")

//Solution with SparSQL

$ gedit sales.txt &
$ hdfs dfs -put /home/cloudera/files/sales.txt /files/
$ hdfs dfs -cat /files/sales.txt

case class Department(department: String, designation: String,cost: Int,state: String)
val departments = sc.textFile("/files/sales.txt").map(line => line.split(",")).map(arr => new Department(arr(0), arr(1), arr(2).toInt, arr(3))).toDF()
departments.registerTempTable("department")
val result = sqlContext.sql("select department,designation,state,count(state) as `empCount`, sum(cost) as `totalCost` from department group by department,designation,state order by state ASC")
val file = result.rdd.map(row => row(0) + "," + row(1) + "," + row(2) + "," + row(3) + "," + row(4))
file.repartition(1).saveAsTextFile("/files/sales")