/**
 * Problem Scenario 69 : Write down a Spark Application using Python,
 * In which it read a file "Content.txt" (On hdfs) with following content.
 * And filter out the word which is less than 2 characters and ignore all empty lines.
 * Once done store the filtered data in a directory called "problem84" (On hdfs)
 * Content.txt
 * Hello this is ABCTECH.com
 * This is ABYTECH.com
 * Apache Spark Training
 * This is Spark Learning Session
 * Spark is faster than MapReduce
 */
//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
$ gedit content.txt &
$ hdfs dfs -put content.txt /files/

//Step 1 : Create an application with following code and store it in problem84.py 
# Import SparkContext and SparkConf 
from pyspark import SparkContext, SparkConf
# Create configuration object and set App name
conf = SparkConf().setAppName("CCA 175 Problem 84")
sc = SparkContext(conf=conf)
#load data from hdfs
rddContent = sc.textFile("/files/content.txt")
#filter out non-empty lines
emptyLines = rddContent.filter(lambda l: l != "")
#Split line based on space
data = emptyLines.flatMap(lambda l: l.split(" "))
#filter out all 2 letter words
dataFilter = data.filter(lambda w: len(w) > 2)
for word in dataFilter.collect(): print(word)
#Save final data 
dataFilter.repartition(1).saveAsTextFile("problem84")

//step 2 : Submit this application 
$ spark-submit --master yarn problem84.py

$ gedit content.txt &
$ hdfs dfs -put content.txt /files/

//Solution on pyspark
$ pyspark
content = sc.textFile("/files/content.txt").filter(lambda w: w != "").flatMap(lambda line: line.split(" ")).filter(lambda w: len(w) > 2)
for word in content.collect(): print(word)
content.repartition(1).saveAsTextFile("problem84")