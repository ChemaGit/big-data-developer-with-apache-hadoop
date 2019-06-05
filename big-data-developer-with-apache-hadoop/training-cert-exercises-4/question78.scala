/** Question 78
 * Problem Scenario 35 : You have been given a file named spark7/EmployeeName.csv (id,name).
 * EmployeeName.csv
 * E01,Lokesh
 * E02,Bhupesh
 * E03,Amit
 * E04,Ratan
 * E05,Dinesh
 * E06,Pavan
 * E07,Tejas
 * E08,Sheela
 * E09,Kumar
 * E10,Venkat
 * 1. Load this file from hdfs and sort it by name and save it back as (id,name) in results directory. However, make sure while saving it should be able to write In a single file.
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution: 
//Step 1 : Create file in hdfs (We will do using Hue). However, you can first create in local filesystem and then upload it to hdfs. 
$ gedit EmployeeName.csv &
$ hdfs dfs -put EmployeeName.csv /files/
$ hdfs dfs -cat /files/EmployeeName.csv

//Step 2 : Load EmployeeName.csv file from hdfs and create PairRDDs 
val name = sc.textFile("/files/EmployeeName.csv") 
val namePairRDD = name.map(x=> (x.split(",")(0),x.split(",")(1))) 

//Step 3 : Now swap namePairRDD RDD. 
val swapped = namePairRDD.map(item => item.swap) 

//step 4: Now sort the rdd by key. 
val sortedOutput = swapped.sortByKey() 

//Step 5 : Now swap the result back 
val swappedBack = sortedOutput.map(item => item.swap) 

//Step 6 : Save the output as a Text file and output must be written in a single file. 
swappedBack.repartition(1).saveAsTextFile("/spark78")

/******ANOTHER WAY TO SOLVE THE PROBLEM WOULD BE...*********/
//Step 1: Create the file and send it from local system to HDFS
$ gedit EmployeeName.csv &
$ hdfs dfs -put EmployeeName.csv /files/
$ hdfs dfs -cat /files/EmployeeName.csv

//Step 2: Load this file from hdfs and sort it by name and save it back as (id,name) in results directory. However, make sure while saving it should be able to write In a single file.
val employee = sc.textFile("/files/EmployeeName.csv").map(line => line.split(",")).map(arr => (arr(1), arr(0))).sortByKey()
val format = employee.map({case(name, id) => "%s,%s".format(id, name)})
format.repartition(1).saveAsTextFile("/spark78")