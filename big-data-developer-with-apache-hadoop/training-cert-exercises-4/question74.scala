/** Question 74
 * Problem Scenario 45 : You have been given 2 files , with the content as given Below
 * (spark12/technology.txt)
 * (spark12/salary.txt)
 * (spark12/technology.txt)
 * first,last,technology
 * Amit,Jain,java
 * Lokesh,kumar,unix
 * Mithun,kale,spark
 * Rajni,vekat,hadoop
 * Rahul,Yadav,scala
 * (spark12/salary.txt)
 * first,last,salary
 * Amit,Jain,100000
 * Lokesh,kumar,95000
 * Mithun,kale,150000
 * Rajni,vekat,154000
 * Rahul,Yadav,120000
 * Write a Spark program, which will join the data based on first and last name and save the joined results in following format, first Last.technology.salary
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1: create the files locally and upload the files from local system to hdfs
$ gedit salary.txt technology.txt &
$ hdfs dfs -put salary.txt technology.txt /files
$ hdfs dfs -cat /files/salary.txt
$ hdfs dfs -cat /files/technology.txt

//Step 2 : Load all file as an RDD 
val technology = sc.textFile("/files/technology.txt").map(e => e.split(",")) 
val salary = sc.textFile("/files/salary.txt").map(e => e.split(",")) 

//Step 3 : Now create Key.value pair of data and join them. 
val joined = technology.map(e=>((e(0),e(1)),e(2))).join(salary.map(e=>((e(0),e(1)),e(2)))) 

//Step 4 : Save the results in a text file as below. 
joined.repartition(1).saveAsTextFile("/spark12")

/******ANOTHER POSSIBLE SOLUTION*********/
//Step 1: create the files locally and upload the files from local system to hdfs
$ gedit salary.txt technology.txt &
$ hdfs dfs -put salary.txt technology.txt /files
$ hdfs dfs -cat /files/salary.txt
$ hdfs dfs -cat /files/technology.txt

//Step 2: Write a Spark program, which will join the data based on first and last name and save the joined results in following format: first,Last,technology,salary
val rddSalary = sc.textFile("/files/salary.txt").map(line => line.split(",")).map(arr => ((arr(0),arr(1)),arr(2)))
val rddTech = sc.textFile("/files/technology.txt").map(line => line.split(",")).map(arr => ((arr(0),arr(1)),arr(2)))
val joined = rddTech.join(rddSalary)
val format = joined.map({case(((f,l),(t,s))) => "%s,%s,%s,%s".format(f,l,t,s)})
format.repartition(1).saveAsTextFile("/files/salaryTech")

//spark-shell -i probe.scala