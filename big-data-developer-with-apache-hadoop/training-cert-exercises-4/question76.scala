/** Question 76
  * Problem Scenario 38 : You have been given an RDD as below,
  * val rdd = sc.parallelize(Array[Array[Byte]]())
  * Now you have to save this RDD as a SequenceFile. And below is the code snippet.
  * import org.apache.hadoop.io.compress.BZip2Codec
  * rdd.map(bytesArray => (A.get(), new B(bytesArray))).saveAsSequenceFile("/user/cloudera/question76/sequence",Some(classOf[BZip2Codec]))
  * What would be the correct replacement for A and B in above snippet.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object question76 {

  val spark = SparkSession
    .builder()
    .appName("question76")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question76")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val output = "hdfs://quickstart.cloudera/user/cloudera/exercises/question_76/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val rdd = sc.parallelize(Array[Array[Byte]]())

      import org.apache.hadoop.io.compress.BZip2Codec

      rdd
        .map(bytesArray => (org.apache.hadoop.io.NullWritable.get(), new org.apache.hadoop.io.BytesWritable()))
        .saveAsSequenceFile(output, Some(classOf[BZip2Codec]))

      /**
        * $ hdfs dfs -ls /user/cloudera/exercises/question_76
        * $ hdfs dfs -text /user/cloudera/exercises/question_76/part*
        */

      // To have the opportunity to view the web console of Spark: http://localhost:4040/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("SparkContext stopped.")
      spark.stop()
      println("SparkSession stopped.")
    }
  }
}

/*SOLUTION IN THE SPARK REPL
val rdd = sc.parallelize(Array[Array[Byte]]())
import org.apache.hadoop.io.compress.BZip2Codec
rdd.map(bytesArray => (org.apache.hadoop.io.NullWritable.get(), new org.apache.hadoop.io.BytesWritable(bytesArray))).saveAsSequenceFile("/user/cloudera/question76/sequence",Some(classOf[BZip2Codec]))

$ hdfs dfs -ls /user/cloudera/question76/sequence
$ hdfs dfs -text /user/cloudera/question76/sequence/*
*/