/**
 * Problem Scenario 70 : Write down a Spark Application using Python, In which it read a
 * file "content.txt" (On hdfs) with following content. Do the word count and save the
 * results in a directory called "problem85" (On hdfs)
 * content.txt
 * Hello this is ABCTECH.com
 * This is XYZTECH.com
 * Apache Spark Training
 * This is Spark Learning Session
 * Spark is faster than MapReduce 
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1 : Create an application with following code and store it in problem84.py 
# Import SparkContext and SparkConf from pyspark 
from pyspark import SparkContext, SparkConf 
# Create configuration object and set App name 
conf = SparkConf().setAppName("CCA 175 Problem 85") 
sc = SparkContext(conf=conf) 
#load data from hdfs 
contentRDD = sc.textFile("/files/content.txt") 
#filter out non-empty lines 
nonemptylines = contentRDD.filter(lambda x: len(x) > 0) 
#Split line based on space 
words = nonemptylines.flatMap(lambda x: x.split(' ')) 
#Do the word count 
wordcounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y).map(lambda x: (x[1], x[0])).sortByKey(False) 
for word in wordcounts.collect(): print(word) 
#Save final data " 
wordcounts.repartition(1).saveAsTextFile("/files/problem85") 
#Stop Sparks
sc.stop()

//step 2 : Submit this application 
spark-submit --master yarn problem84.py

//Solution in pyspark
$ gedit /home/training/Desktop/files/content.txt &
$ hdfs dfs -put /home/training/Desktop/files/content.txt /files

rdd = sc.textFile("/files/content.txt").flatMap(lambda line: line.split(' '))
rrdCount = rdd.map(lambda word: (word, 1)).reduceByKey(lambda v, v1: v + v1)
rddCount.collect()
rddCount.repartition(1).saveAsTextFile("/files/problem85")