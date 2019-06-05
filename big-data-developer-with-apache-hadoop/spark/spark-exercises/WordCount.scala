package spark.spark-exercises

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.nio.file.Paths

object WordCount {
  
  /** @return The filesystem path of the given resource */
  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString   
  
    //resource ==> /wordcount.txt
	def main(args: Array[String]): Unit = {
	  if(args.length < 1) {
	    System.err.println("Usage: spark.WordCount <file>")
	    System.exit(1)
	  }
	  val resource = fsPath(args(0))
	  val sconf = new SparkConf
	  sconf.setAppName("WordCount")
	  sconf.setMaster("local")
	  val sc = new SparkContext(sconf)
	  sc.setLogLevel("ERROR")
	  
	  val counts = sc.textFile(resource, 1)
	                 .flatMap(line => line.split("\\W"))
	                 .map(word => (word, 1))
	                 .reduceByKey((v, v1) => v + v1)

	  counts.take(50).foreach(println)
	  
	  sc.stop()
	}
}