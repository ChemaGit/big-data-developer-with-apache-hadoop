import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]) {
    if(args.length < 1) {
      System.err.println("Usage: WordCount <file>")
      System.exit(1)
    }
    
    val sc = new SparkContext()
    
    val counts = sc.textFile(args(0))
                   .flatMap(line => line.split("\\W"))
                   .map(word => (word, 1))
                   .reduceByKey({case(v, v1) => v + v1})
                   
    counts.take(5).foreach(println)
    
    sc.stop()
  }
}