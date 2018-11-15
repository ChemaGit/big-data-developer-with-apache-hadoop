/** Question 71
 * Problem Scenario 31 : You have given following two files
 * 1. Content.txt: Contain a huge text file containing space separated words.
 * 2. Remove.txt: Ignore/filter all the words given in this file (Comma Separated).
 * Write a Spark program which reads the Content.txt file and load as an RDD, remove all the
 * words from a broadcast variables (which is loaded as an RDD of words from Remove.txt).
 * And count the occurrence of the each word and save it as a text file in HDFS.
 * Content.txt
 * Hello this is ABCTech.com
 * This is TechABY.com
 * Apache Spark Training
 * This is Spark Learning Session
 * Spark is faster than MapReduce
 * Remove.txt
 * Hello, is, this, the
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1 : Create all the files in hdfs in directory called spark2 (We will do using Hue). 
//However, you can first create in local filesystem and then upload it to hdfs 
$ gedit Content.txt Remove.txt &
$ hdfs dfs -put Content.txt Remove.txt /files

//Step 2 : Load the Content.txt file 
val content = sc.textFile("/files/Content.txt") 
 
//Step 3 : Load the Remove.txt file 
val remove = sc.textFile("/files/Remove.txt")  

//Step 4 : Create an RDD from remove, However, there is a possibility each word could have trailing spaces, remove those whitespaces as well. 
//We have used two functions here flatMap, map and trim. 
val removeRDD = remove.flatMap(x=> x.split(",") ).map(word => word.trim)

//Step 5 : Broadcast the variable, which you want to ignore 
val bRemove = sc.broadcast(removeRDD.collect().toList) 

//Step 6 : Split the content RDD, so we can have Array of String. 
val words = content.flatMap(line => line.split(" ")) 

//Step 7 : Filter the RDD, so it can have only content which are not present in "Broadcast Variable". 
val filtered = words.filter(word => !bRemove.value.contains(word)) 

//Step 8 : Create a PairRDD, so we can have (word,1) tuple or PairRDD. 
val pairRDD = filtered.map(word => (word,1)) 

//Step 9 : Nowdo the word count on PairRDD. 
val wordCount = pairRDD.reduceByKey(_ + _) 

//Step 10 : Save the output as a Text file. 
wordCount.repartition(1).saveAsTextFile("/files/spark68/result")

/*******WITHOUT BROADCAST SOLUTION***********/
//Step 1: Create the files and send them to HDFS
$ gedit Content.txt Remove.txt &
$ hdfs dfs -put Content.txt Remove.txt /files

//Step 2: Write a Spark program which reads the Content.txt file and load as an RDD
val rddContent = sc.textFile("/files/Content.txt").flatMap(line => line.split(" ")).filter(w => w != "" || w != " ")
val rddRemove = sc.textFile("/files/Remove.txt").flatMap(line => line.split(",")).map(w => w.trim)

//Step 3: remove all the words from a broadcast variables (which is loaded as an RDD of words from Remove.txt)
val filtered = rddContent.subtract(rddRemove)

//Step 4: count the occurrence of the each word and save it as a text file in HDFS.
val counted = filtered.map(w => (w, 1)).reduceByKey((v, v1) => v + v1)
counted.repartition(1).saveAsTextFile("/files/spark68")