/** Question 93
 * Problem Scenario 36 : You have been given a file named spark8/data.csv (type,name).
 * data.csv
 * 1,Lokesh
 * 2,Bhupesh
 * 2,Amit
 * 2,Ratan
 * 2,Dinesh
 * 1,Pavan
 * 1,Tejas
 * 2,Sheela
 * 1,Kumar
 * 1,Venkat
 * 1. Load this file from hdfs and save it back as (id, (all names of same type)) in results directory. However, make sure while saving it should be only one file.
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1 : Create file in hdfs (We will do using Hue). However, you can first create in local filesystem and then upload it to hdfs. 
$ touch data.csv
$ gedit data.csv &
$ hdfs dfs -put data.csv /files/
$ hdfs dfs -cat /files/data.csv

//Step 2 : Load data.csv file from hdfs and create PairRDDs
val name = sc.textFile("/files/data.csv") 
val namePairRDD = name.map(x=> (x.split(",")(0),x.split(",")(1))) 
//Step 3 : Now swap namePairRDD RDD. 
val swapped = namePairRDD.map(item => item.swap) 

//Step 4 : Now combine the rdd by key. 
val combinedOutput = namePairRDD.combineByKey(List(_), (x:List[String], y:String) => y :: x, (x:List[String], y:List[String]) => x ::: y) 

//Step 5 : Save the output as a Text file and output must be written in a single file. 
combinedOutput.repartition(1).saveAsTextFile("/files/spark91")

/**********A BETTER SOLUTION**************/
//Step 1: Create the file data.csv and send it from local system to hdfs
$ touch data.csv
$ gedit data.csv &
$ hdfs dfs -put data.csv /files/
$ hdfs dfs -cat /files/data.csv

//Step 2: make the program in Scala Spark
val data = sc.textFile("/files/data.csv").map(lines => lines.split(",")).map(arr => (arr(0), arr(1))).groupByKey().map(t => (t._1, "(" + t._2.mkString(",") + ")"))
data.repartition(1).saveAsTextFile("/files/spark90")

//Step 3: make the program in Pyspark
data = sc.textFile("/files/data.csv").map(lambda lines: lines.split(",")).map(lambda arr: (arr[0], arr[1])).reduceByKey(lambda x, x1: x + "," + x1).map(lambda t: (t[0], "({0})".format(t[1])))
data.repartition(1).saveAsTextFile("/files/spark91")