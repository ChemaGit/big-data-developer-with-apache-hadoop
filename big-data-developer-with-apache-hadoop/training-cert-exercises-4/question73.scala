/** Question 73
 * Problem Scenario 71 :
 * Write down a Spark script using Python,
 * In which it read a file "Content.txt" (On hdfs) with following content.
 * After that split each row as (key, value), where key is first word in line and entire line as value.
 * Filter out the empty lines.
 * And save this key value in "problem86" as Sequence file(On hdfs)
 * Part 2 : Save as sequence file , where key as null and entire line as value. Read back the stored sequence files.
 * Content.txt
 * Hello this is ABCTECH.com
 * This is XYZTECH.com
 * Apache Spark Training
 * This is Spark Learning Session
 * Spark is faster than MapReduce
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution :
//First: create the file and upload from local system to HDFS
$ gedit Content.txt &
$ hdfs dfs -put Content.txt /files/
$ hdfs dfs -cat /files/Content.txt
 
//Step 1 : 
# Import SparkContext and SparkConf 
from pyspark import SparkContext, SparkConf 

# Create configuration object and set App name 
conf = SparkConf().setAppName("CCA 175 Problem 85") 
sc = SparkContext(conf=conf) 

//Step 2: 
#load data from hdfs 
contentRDD = sc.textFile("/files/Content.txt") 

//Step 3: 
#filter out non-empty lines 
nonempty_lines = contentRDD.filter(lambda x: len(x) > 0) 

//Step 4: 
#Split line based on space (Remember : It is mandatory to convert is in tuple) 
words = nonempty_lines.map(lambda x: (x.split()[0],x)) 
words.repartition(1).saveAsSequenceFile("/files/problem86") 

for w in words.collect():print(w)

//Step 5: Check contents in directory problem86 
$ hdfs dfs -cat problem86/p* 

//Step 6 : 
#Create key, value pair (where key is null) 
nonempty_lines.map(lambda line: (None, line)).saveAsSequenceFile("/files/problem86_1") 

//Step 7 : 
#Reading back the sequence file data using spark. 
seqRDD = sc.sequenceFile("/files/problem86_1") 

//Step 8 : 
#Print the content to validate the same. 
for line in seqRDD.collect(): print(line)

#Stop Sparks
sc.stop()

#spark-submit --master yarn problem86.py