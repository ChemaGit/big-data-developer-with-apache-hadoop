// $ mvn package
// $ --spark-submit --class example.CheckPointDir --name 'CheckPointDir' --master yarn-client target/checkpoint-1.0.jar /files/checkPointDir
// $ --spark-submit --class example.CheckPointDir --name 'CheckPointDir' --master 'local[*]' target/checkpoint-1.0.jar /files/checkPointDir
// $ --spark-submit --class example.CheckPointDir --name 'CheckPointDir' --master 'local[8]' target/checkpoint-1.0.jar /files/checkPointDir

package example

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import java.lang.Exception
import java.lang.RuntimeException

object CheckPointDir {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: example.CheckPointDir <dir>") // "/files/checkPointDir"
      System.exit(1)
    }

    val sconf = new SparkConf().setAppName("CheckPointDir").set("spark.ui.port","4141")
    val sc = new SparkContext(sconf)
    sc.setLogLevel("ERROR")

    sc.setCheckpointDir(args(0))	
    
    try{
      val rddEnteros = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))

      for (i <- 1 to 1001) {
        val sumRddEnteros = rddEnteros.map(x => x + 1)
	      if (i % 10 == 0) {
	        println("Bucle numero " + i)
	        //Before any actions "checkpoint"
	        sumRddEnteros.checkpoint()
	        sumRddEnteros.count()
          println("sumRddEnteros.count(): " + sumRddEnteros.count())
	        println("sumRddEnteros: " + sumRddEnteros.collect().mkString(",") )
	      }
      }
    } catch {
        case ex: Exception => println("Exception: " + ex.toString())
        case ex: RuntimeException => println("RuntimeException: " + ex.toString())
    }
    sc.stop()
  }
}