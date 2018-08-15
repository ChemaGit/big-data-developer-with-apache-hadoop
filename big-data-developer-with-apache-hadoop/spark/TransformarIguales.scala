package spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.nio.file.Paths

object TransformarIguales {
  
  def fsPath(resource: String): String = {
    Paths.get(getClass.getResource(resource).toURI).toString()
  }
  
	def main(args: Array[String]) = {
		if(args.length <= 0) {
		  System.err.println("usage: spark.TransformarIguales <file>")
		  System.exit(1)
		}
		
		val sconf = new SparkConf
		sconf.setAppName("TransformarIguales")
		sconf.setMaster("local")
		val sc = new SparkContext(sconf)
		sc.setLogLevel("ERROR")
		
		val resource = fsPath(args(0))
		println("MotherFucker")
		println(resource)
		val rdd = sc.textFile(resource, 1)
		            .map(line => ( (line.split("=")(0), line.split("=")(1)), 1 ))
		            .reduceByKey((v, v1) => v + v1)
		            .sortBy({case(k,v) => v}, false, 1)
		            .map({case(k, v) => ("(" + k._1 + "," + k._2 + "," + v + ")")})
		//rdd.saveAsTextFile("C:\\temp\\output\\")            				
		rdd.collect().foreach(println)
		sc.stop()
	}
}