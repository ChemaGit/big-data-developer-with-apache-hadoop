/** Question 76
  * Problem Scenario 38 : You have been given an RDD as below,
  * val rdd = sc.parallelize(Array[Array[Byte]]())
  * Now you have to save this RDD as a SequenceFile. And below is the code snippet.
  * import org.apache.hadoop.io.compress.BZip2Codec
  * rdd.map(bytesArray => (A.get(), new B(bytesArray))).saveAsSequenceFile("/user/cloudera/question76/sequence",Some(classOf[BZip2Codec]))
  * What would be the correct replacement for A and B in above snippet.
  */
val rdd = sc.parallelize(Array[Array[Byte]]())
import org.apache.hadoop.io.compress.BZip2Codec
rdd.map(bytesArray => (org.apache.hadoop.io.NullWritable.get(), new org.apache.hadoop.io.BytesWritable(bytesArray))).saveAsSequenceFile("/user/cloudera/question76/sequence",Some(classOf[BZip2Codec]))

$ hdfs dfs -ls /user/cloudera/question76/sequence
$ hdfs dfs -text /user/cloudera/question76/sequence/*