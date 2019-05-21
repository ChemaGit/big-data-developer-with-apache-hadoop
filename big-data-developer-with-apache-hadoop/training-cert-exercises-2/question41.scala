/** Question 41
  * Problem Scenario 70 : Write down a Spark Application using Scala, In which it read a
  * file "Content.txt" (On hdfs) with following content. Do the word count and save the
  * results in a directory called "question41" (On hdfs)
  * content.txt
  * Hello this is ABCTECH.com
  * This is XYZTECH.com
  * Apache Spark Training
  * This is Spark Learning Session
  * Spark is faster than MapReduce
  */
// Scala
val filtered = List("", " ")
val content = sc.textFile("/user/cloudera/files/Content.txt").flatMap(line => line.split("\\W")).filter(w => !filtered.contains(w))
val result = content.map(w => (w, 1)).reduceByKey( (v, c) => v + c).sortBy(t => t._2, false)

result.repartition(1).saveAsTextFile("/user/cloudera/question41/result")

$ hdfs dfs -ls hdfs dfs -ls /user/cloudera/question41/result/
  $ hdfs dfs -cat /user/cloudera/question41/result/part-*