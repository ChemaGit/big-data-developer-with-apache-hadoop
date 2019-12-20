/** Question 13
  * Problem Scenario 32 : You have given three files as below.
  * spark3/sparkdir1/file1.txt
  * spark3/sparkdir2/file2.txt
  * spark3/sparkdir3/file3.txt
  * Each file contain some text.
  * spark3/sparkdir1/file1.txt
  * Apache Hadoop is an open-source software framework written in Java for distributed
  * storage and distributed processing of very large data sets on computer clusters built from
  * commodity hardware. All the modules in Hadoop are designed with a fundamental
  * assumption that hardware failures are common and should be automatically handled by the framework
  * spark3/sparkdir2/file2.txt
  * The core of Apache Hadoop consists of a storage part known as Hadoop Distributed File
  * System (HDFS) and a processing part called MapReduce. Hadoop splits files into large
  * blocks and distributes them across nodes in a cluster. To process data, Hadoop transfers
  * packaged code for nodes to process in parallel based on the data that needs to be processed.
  * spark3/sparkdir3/file3.txt
  * his approach takes advantage of data locality nodes manipulating the data they have
  * access to to allow the dataset to be processed faster and more efficiently than it would be
  * in a more conventional supercomputer architecture that relies on a parallel file system
  * where computation and data are distributed via high-speed networking
  *
  * Now write a Spark code in scala which will load all these three files from hdfs and do the
  * word count by filtering following words. And result should be sorted by word count in reverse order.
  * Filter words ("a","the","an", "as", "a","with","this","these","is","are","in", "for","to","and","The","of",""," ")
  * Also please make sure you load all three files as a Single RDD (All three files must be loaded using single API call).
  * You have also been given following codec
  * import org.apache.hadoop.io.compress.GzipCodec
  * Please use above codec to compress file, while saving in hdfs.
  */
$ hdfs dfs -put /home/cloudera/files/file1.txt /user/cloudera/files
$ hdfs dfs -put /home/cloudera/files/file2.txt /user/cloudera/files
$ hdfs dfs -put /home/cloudera/files/file3.txt /user/cloudera/files

val filtered = List("a","the","an", "as", "a","with","this","these","is","are","in", "for","to","and","The","of",""," ")

val file = sc.textFile("/user/cloudera/files/file1.txt,/user/cloudera/files/file2.txt,/user/cloudera/files/file3.txt").flatMap(line => line.split("\\W"))
val filterWord = file.filter(w => !filtered.contains(w))
val countWord = filterWord.map(w => (w, 1)).reduceByKey( (v,t) => v + t)
val sortDesc = countWord.sortBy(t => t._2, false)
sortDesc.saveAsTextFile("/user/cloudera/question13/result",classOf[org.apache.hadoop.io.compress.GzipCodec])

$ hdfs dfs -ls /user/cloudera/question13/result
$ hdfs dfs -text /user/cloudera/question13/result/part-00000.gz
