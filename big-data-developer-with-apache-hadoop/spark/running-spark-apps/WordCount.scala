//running the app locally
// $ cd wordcount_project/
// $ ls  ===>> pom.xml   src   target
// $ mvn package
// $ spark-submit --class example.WordCount target/wordcount-1.0.jar purplecow.txt
// $ spark-submit --class example.WordCount --master 'local[*]' target/wordcount-1.0.jar purplecow.txt
// $ spark-submit --class example.WordCount --master 'local[8]' target/wordcount-1.0.jar purplecow.txt

//running the application on the YARN cluster
// &spark-submit --class example.WordCount --master yarn-client target/wordcount-1.0.jar purplecow.txt 

package example

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: example.WordCount <file>")
      System.exit(1)
    }

    val sconf = new SparkConf().setAppName("Word Count").set("spark.ui.port","4141")
    val sc = new SparkContext(sconf)
    sc.setLogLevel("ERROR")

    val file = args(0)
    val counts = sc.textFile(file).
       flatMap(line => line.split("\\W")).
       map(word => (word,1)).
       reduceByKey((v1,v2) => v1+v2)
    println("App-name: " + sconf.get("spark.app.name"))
    counts.take(10).foreach(println)
    
    sc.stop()
  }
}
