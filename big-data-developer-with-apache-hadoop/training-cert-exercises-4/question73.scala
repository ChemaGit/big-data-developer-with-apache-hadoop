/** Question 73
  * Problem Scenario 71 :
  * Write down a Spark script using Scala,
  * In which it read a file "Content.txt" (On hdfs) with following content.
  * After that split each row as (key, value), where key is first word in line and entire line as value.
  * Filter out the empty lines.
  * And save this key value in "question73" as Sequence file(On hdfs)
  * Part 2 : Save as sequence file , where key as null and entire line as value. Read back the stored sequence files.
  * Content.txt
  * Hello this is ABCTECH.com
  * This is XYZTECH.com
  * Apache Spark Training
  * This is Spark Learning Session
  * Spark is faster than MapReduce
  */
$ gedit /home/cloudera/files/Content.txt
$ hdfs dfs -put /home/cloudera/files/Content.txt /user/cloudera/files

val content = sc.textFile("/user/cloudera/files/Content.txt").map(line => (line.split(" ")(0),line))
content.saveAsSequenceFile("/user/cloudera/files/question73/sequence")

val sequence = sc.sequenceFile("/user/cloudera/files/question73/sequence",classOf[org.apache.hadoop.io.Text],classOf[org.apache.hadoop.io.Text])
val printSequence = sequence.map(t => (t._1.toString, t._2.toString))
printSequence.collect.foreach(println)