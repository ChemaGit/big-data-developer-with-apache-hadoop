package spark.spark-exercises

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.nio.file.Paths

/**
 * Problem Scenario 39 : You have been given two files
 * spark16/file1.txt
 * 1,9,5
 * 2,7,4
 * 3,8,3
 * spark16/file2.txt
 * 1,g,h
 * 2,i,j
 * 3,k,l
 * Load these two tiles as Spark RDD and join them to produce the below results
 * (l,((9,5),(g,h)))
 * (2, ((7,4), (i,j))) (3, ((8,3), (k,l)))
 * And write code snippet which will sum the second columns of above joined results (5+4+3).
 */
object Spark16 {  
  
  /** @return The filesystem path of the given resource */
  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI()).toString() + "/*"  
  
	def main(args: Array[String]): Unit = {
		if(args.length <= 0) {
		  System.err.println("usage: runMain spark.Spark16 /spark16")
		  System.exit(1)
		}
		
		val resource = args(0)
		
		println(resource)
		println()
		
		val dir = fsPath(resource) 
		
		println(dir)
		println
		
		val sconf = new SparkConf
		sconf.setAppName("Spark16")
		sconf.setMaster("local")
		val sc = new SparkContext(sconf)
		sc.setLogLevel("ERROR")
		
		val rdd  = sc.textFile(dir)		
		//rdd.collect().foreach(line => println(line))
		
		val rdd2 = rdd.map(line => line.split(","))
		              .map(arr => (arr(0),(arr(1), arr(2))))
		//rdd2.collect().foreach(line => println(line)) 
		val result = rdd2.groupByKey().map({case(k, v) => v.toList(0)._2.toInt}).reduce({case(v, v1) => v + v1})
		println("Y el resultado es: " + result)
		
		sc.stop()
	}
}