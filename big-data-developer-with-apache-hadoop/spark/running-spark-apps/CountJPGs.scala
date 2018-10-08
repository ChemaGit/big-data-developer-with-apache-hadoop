//running the app locally
// $ cd countjpgs_project/
// $ ls  ===>> pom.xml   src   target
// $ mvn package
// $ spark-submit --class stubs.CountJPGs target/countjpgs-1.0.jar /loudacre/weblogs/*
// $ spark-submit --class stubs.CountJPGs --master 'local[*]' target/countjpgs-1.0.jar /loudacre/weblogs/*
// $ spark-submit --class stubs.CountJPGs --master 'local[8]' target/countjpgs-1.0.jar /loudacre/weblogs/*


//running the application on the YARN cluster
// $ spark-submit --class stubs.CountJPGs --master yarn-client target/countjpgs-1.0.jar /loudacre/weblogs/*

package stubs
//dir ===>  countjpgs_project/src/main/scala/stubs
import org.apache.spark.SparkContext

object CountJPGs {
   def main(args: Array[String]) {
     if (args.length < 1) {
       System.err.println("Usage: stubs.CountJPGs <logfile>")
       System.exit(1)
     }     	
     //TODO: complete exercise
     val sc = new SparkContext()
     sc.setLogLevel("ERROR")
     val dir = args(0)
     val rdd = sc.textFile(dir).filter(line => line.contains(".jpg"))
     println("Num of requests for .jpg: " + rdd.count())
     sc.stop()
   }
 }
