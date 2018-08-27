package spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.nio.file.Paths

object IpPorCiudad {
  
  def fspath(dir: String): String = {
    val path = Paths.get(getClass.getResource(dir).toURI()).toString()
    path
  }
  
  def ciudad(ip: String): String = {
    val n = ip.split('.')(0).toInt
    if(n < 64) "Palencia"
    else if(n < 128) "Orense"
    else if(n < 192) "Huelva"
    else "Santander"
  }
  
	def main(args: Array[String]): Unit = {
	  //runMain spark.IpPorCiudad /weblogs
	  if(args.length <= 0) {
	    System.err.println("Usage: spark.IpPorCiudad <directory>")
	    System.exit(1)
	  }
	  
	  val resource = fspath(args(0)) + "/*"
	  // /observatory/src/main/resources/weblogs/*
		val sconf = new SparkConf
		sconf.setAppName("IpPorCiudad")
		sconf.setMaster("local")
		val sc = new SparkContext(sconf)
		sc.setLogLevel("ERROR")
	
	  val rddInit = sc.textFile(resource).map(line => (ciudad(line.split(' ')(0)), 1) )
	  val rdd = rddInit.reduceByKey((v1, v2) => v1 + v2)
	  val result = rdd.sortBy({case(c, n) => n}, ascending = false, 1)		
		result.foreach(println)
		//print the result in one file
		//result.repartition(1).saveAsTextFile("ipPorCiudad")	
		sc.stop()
	}
}