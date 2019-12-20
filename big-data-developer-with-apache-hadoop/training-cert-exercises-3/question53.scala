/** Question 53
  * Problem Scenario 69 : Write down a Spark Application using Scala,
  * In which it read a file "Content.txt" (On hdfs) with following content.
  * And filter out the word which is less than 3 characters and ignore all empty lines.
  * Once done store the filtered data in a directory called "question53" (On hdfs)
  * Content.txt
  * Hello this is ABCTECH.com
  * This is ABYTECH.com
  * Apache Spark Training
  * This is Spark Learning Session
  * Spark is faster than MapReduce
  */
val filt = List("", " ")
val content = sc.textFile("/user/cloudera/files/Content.txt").flatMap(line => line.split(" ")).filter(w => !filt.contains(w)).filter(w => w.length > 2)
content.saveAsTextFile("/user/cloudera/question53")

$ hdfs dfs -ls /user/cloudera/question53
$ hdfs dfs -cat /user/cloudera/question53/part-00000