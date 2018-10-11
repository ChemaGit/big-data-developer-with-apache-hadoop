import org.apache.spark.SparkContext

//mvn package
//spark-submit --class AvgWordLengthByFirstLetter --name 'AverageWordLengthByFirstLetter' --master yarn-client target/avg-length-1.0.jar /loudacre/frostroad.txt

object AvgWordLengthByFirstLetter {
   def main(args: Array[String]) {
    	if(args.length < 1) {
    		System.err.println("Usage: AvgWordLengthByFirstLetter <file>")
    		System.exit(1)
    	}
    	val sc = new SparkContext()
    	sc.setLogLevel("ERROR")
    	// specify 4 partitions to simulate a 4-block file
    	val rdd = sc.textFile(args(0), 4)
    	val avglens = rdd.flatMap(line => line.split("\\W"))
    			 .filter(line => line.length > 0)
                             .map(word => (word(0), word.length))
                             .groupByKey(1) //specify one partition
                             .mapValues(it => it.sum / it.size)
          	// call save to trigger the operations
    	avglens.saveAsTextFile("/loudacre/avglens")
    
    	sc.stop()
   }
}