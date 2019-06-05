/** Question 86
 * Problem Scenario 44 : You have been given 4 files , with the content as given below:
 * spark11/file_1.txt
 * Apache Hadoop is an open-source software framework written in Java for distributed
 * storage and distributed processing of very large data sets on computer clusters built from
 * commodity hardware. All the modules in Hadoop are designed with a fundamental
 * assumption that hardware failures are common and should be automatically handled by the framework
 * spark11/file_2.txt
 * The core of Apache Hadoop consists of a storage part known as Hadoop Distributed File
 * System (HDFS) and a processing part called MapReduce. Hadoop splits files into large
 * blocks and distributes them across nodes in a cluster. To process data, Hadoop transfers
 * packaged code for nodes to process in parallel based on the data that needs to be processed.
 * spark11/file_3.txt
 * his approach takes advantage of data locality nodes manipulating the data they have
 * access to to allow the dataset to be processed faster and more efficiently than it would be
 * in a more conventional supercomputer architecture that relies on a parallel file system
 * where computation and data are distributed via high-speed networking
 * spark11/file_4.txt
 * Apache Storm is focused on stream processing or what some call complex event
 * processing. Storm implements a fault tolerant method for performing a computation or
 * pipelining multiple computations on an event as it flows into a system. One might use
 * Storm to transform unstructured data as it flows into a system into a desired format
 * (spark11/file_1.txt)
 * (spark11/file_2.txt)
 * (spark11/file_3.txt)
 * (spark11/file_4.txt)
 * Write a Spark program, which will give you the highest occurring words in each file. With their file name and highest occurring words.
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1 : Create all 4 file first using Hue in hdfs. 
$ gedit file_1.txt file_2.txt file_3.txt file_4.txt &
$ hdfs dfs -mkdir /files/spark11/
$ hdfs dfs -put file_1.txt file_2.txt file_3.txt file_4.txt /files/spark11/
$ hdfs dfs -ls /files/spark11/
$ hdfs dfs -cat /files/spark11/file_4.txt

//Step 2 : Load all file as an RDD 
val file1 = sc.textFile("/files/spark11/file_1.txt") 
val file2 = sc.textFile("/files/spark11/file_2.txt") 
val file3 = sc.textFile("/files/spark11/file_3.txt") 
val file4 = sc.textFile("/files/spark11/file_4.txt") 

//Step 3 : Now do the word count for each file and sort in reverse order of count. 
val content1 = file1.flatMap( line => line.split(" ")).map(word => (word,1)).reduceByKey(_ + _).map(item => item.swap).sortByKey(false).map(e=>e.swap).collect 
val content2 = file2.flatMap( line => line.split(" ")).map(word => (word,1)).reduceByKey(_ + _).map(item => item.swap).sortByKey(false).map(e=>e.swap).collect  
val content3 = file3.flatMap( line => line.split(" ")).map(word => (word,1)).reduceByKey(_ + _).map(item => item.swap).sortByKey(false).map(e=>e.swap).collect  
val content4 = file4.flatMap( line => line.split(" ")).map(word => (word,1)).reduceByKey(_ + _).map(item => item.swap).sortByKey(false).map(e=>e.swap).collect  

//Step 4 : Split the data and create RDD of all Employee objects. 
val file1word = sc.makeRDD(Array(file1.name+"->"+content1(0)._1+"-"+content1(0)._2)) 
val file2word = sc.makeRDD(Array(file2.name+"->"+content2(0)._1+"-"+content2(0)._2)) 
val file3word = sc.makeRDD(Array(file3.name+"->"+content3(0)._1+"-"+content3(0)._2)) 
val file4word = sc.makeRDD(Array(file4.name+"->"+content4(0)._1+"-"+content4(0)._2)) 

//Step 5: Union all the RDDS 
val unionRDDs = file1word.union(file2word).union(file3word).union(file4word) 

//Step 6 : Save the results in a text file as below. 
unionRDDs.repartition(1).saveAsTextFile("/files/spark11/union")

/****ANOTHER SOLUTION***********/
//Step 1: Create the files and upload them to HDFS
$ gedit file_1.txt file_2.txt file_3.txt file_4.txt &
$ hdfs dfs -mkdir /files/spark11/
$ hdfs dfs -put file_1.txt file_2.txt file_3.txt file_4.txt /files/spark11/
$ hdfs dfs -ls /files/spark11/
$ hdfs dfs -cat /files/spark11/file_4.txt

//Step 2: Write a Spark program, which will give you the highest occurring words in each file. With their file name and highest occurring words.
val files = sc.wholeTextFiles("/files/spark11/*")
val arrs = files.map({case(k, v) => (k, v.split("\\W").filter(w => !w.isEmpty)) })
var max = 0
val grouped = arrs.mapValues(v => v.groupBy(t => t).values.map({case(arr) => {if(arr.length > max) max = arr.length; arr}}).filter(arr => arr.length >= max))
val sorted = grouped.mapValues(v => v.toList.map(arr => (arr(0), arr.length)).sortBy(t => t._1.toLowerCase) ).mapValues({case(l) => if(!l.isEmpty) l.head})
sorted.collect.foreach({case( (n,(w,o)) ) => println("file: %s, word: %s, occurrences: %d".format(n,w,o))})
sorted.repartition(1).saveAsTextFile("/files/spark11/result")