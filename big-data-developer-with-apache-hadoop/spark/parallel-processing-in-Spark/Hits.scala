import org.apache.spark.SparkContext

//mvn package
//spark-submit --class Hits --name 'WebPages Hits By IdUser' --master yarn-client target/hits-1.0.jar /loudacre/accounts/* /loudacre/weblogs/*2.log

object Hits {
   def main(args: Array[String]) {
    	if(args.length < 2) {
    		System.err.println("Usage: Hits </loudacre/accounts/*> </loudacre/weblogs/*2.log>")
    		System.exit(1)
    	}
    	val sc = new SparkContext()
    	sc.setLogLevel("ERROR")
    	//Create an RDD of accounts, keyed by ID and with the string: first_name, last_name for the value
            val rdd = sc.textFile(args(0)).map(line => line.split(",")).map(arr => (arr(0),arr(4) + "," + arr(3)) )
    	//Construct a userReqs RDD with the total number of web hits for each user ID:
    	val userReqs = sc.textFile(args(1)).map(line => line.split(" ")).map(arr => (arr(2), 1)).reduceByKey((v1, v2) => v1 + v2)
    	//Then join the two RDDs by user ID, and construct a new RDD with first name, last name, and total hits:
    	val join = rdd.join(userReqs)
            val res = join.map(pair => pair._2)
    	//Print the results of res.toDebugString and review the output.
    	res.toDebugString
    	res.saveAsTextFile("/loudacre/hits")
    	sc.stop()
   }
}
