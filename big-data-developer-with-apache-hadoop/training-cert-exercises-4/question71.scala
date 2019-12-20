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
$ gedit /home/cloudera/files/Content.txt
$ gedit /home/cloudera/files/Remove.txt
$ hdfs dfs -put /home/cloudera/files/Content.txt /user/cloudera/files
$ hdfs dfs -put /home/cloudera/files/Remove.txt /user/cloudera/files

val l = List("", " ")
val remove = sc.textFile("/user/cloudera/files/Remove.txt").map(line => line.split(",")).collect
val broadcast = sc.broadcast(remove(0).toList.map(v => v.trim))
val content = sc.textFile("/user/cloudera/files/Content.txt").flatMap(line => line.split(" ")).filter(w => !l.contains(w)).filter(w => !broadcast.value.contains(w))
val wordCount = content.map(w => (w,1)).reduceByKey( (v,c) => v + c).sortBy(t => t._2, false)
wordCount.saveAsTextFile("/user/cloudera/question71/result")

$ hdfs dfs -cat /user/cloudera/question71/result/part*