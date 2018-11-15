/** Question 64
 * Problem Scenario 34 : You have given a file named spark6/user.csv.
 * Data is given below:
 * user.csv
 * id,topic,hits
 * Rahul,scala,120
 * Nikita,spark,80
 * Mithun,spark,1
 * myself,cca175,180
 * Now write a Spark code in scala which will remove the header part and create RDD of values as below, for all rows. And also if id is myself" than filter out row.
 * Map(id -> om, topic -> scala, hits -> 120)
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1 : Create file in hdfs (We will do using Hue). However, you can first create in local filesystem and then upload it to hdfs. 
$ gedit user.csv &
$ hdfs dfs -put user.csv /files/

//Step 2 : Load user.csv file from hdfs and create PairRDDs 
val csv = sc.textFile("/files/user.csv") 

//Step 3 : split and clean data 
val headerAndRows = csv.map(line => line.split(",").map(_.trim)) 

//Step 4 : Get header row 
val header = headerAndRows.first 

//Step 5 : Filter out header (We need to check if the first val matches the first header name) 
val data = headerAndRows.filter(_(0) != header(0)) 

//Step 6 : Splits to map (header/value pairs) 
val maps = data.map(splits => header.zip(splits).toMap) 

//step 7: Filter out the user "myself" 
val result = maps.filter(map => map("id") != "myself") 

//Step 8 : Save the output as a Text file. 
result.repartition(1).saveAsTextFile("/files/spark65")

/*********OTHER SOLUTION************/
//Step 1: We create the file user.csv and move the file from local system to HDFS
$ gedit user.csv &
$ hdfs dfs -put user.csv /files/

//Step 2: Check the file
$ hdfs dfs -cat /files/user.csv

//Step 3: code in Scala to solve the problemfil
val rdd = sc.textFile("/files/user.csv").map(line => line.split(","))
val header = rdd.first
val filtered = rdd.filter(arr => arr(0) != header(0)).filter(arr => arr(0) != "myself")
val format = filtered.map(arr => List( (header(0),arr(0)), (header(1),arr(1)), (header(2),arr(2)) ).toMap )
format.repartition(1).saveAsTextFile("/files/spark64")