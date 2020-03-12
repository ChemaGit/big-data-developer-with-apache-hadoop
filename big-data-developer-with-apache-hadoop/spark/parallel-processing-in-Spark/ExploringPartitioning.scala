/**
	* EXPLORING PARTITIONING OF FILE-BASED-RDDs
	*/

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ExploringPartitioning {

	val spark = SparkSession
		.builder()
		.appName("ExploringPartitioning")
		.master("local[*]")
		.config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
		.config("spark.app.id", "ExploringPartitioning") // To silence Metrics warning
		.getOrCreate()

	val sc = spark.sparkContext

	def main(args: Array[String]): Unit = {

		Logger.getRootLogger.setLevel(Level.ERROR)

		try {

			//	1)Run in local mode with three threads
			//		$ spark-shell --master 'local[3]'
			//	2)Take a note of the number of files in /loudacre/accounts/
			//		five files
			//	3)Create an RDD based on a single file, then call toDebugString. How many partitions are in the resulting RDD?
			val accounts = sc.textFile("/loudacre/accounts/part-m-00000")
			accounts.toDebugString ==> one partition
			// 	4)Repeat the process, but specify a minimum of three partitions:
			val rdd = sc.textFile("/loudacre/accounts/part-m-00000", 3)
			rdd.toDebugString
			//		Does the RDD correctly have three partitions? Yes
			//	5)Set the accounts variable to a new RDD based on all files in the accounts dataset.
			//    	  How does the number of files in the dataset compare to the number of partitions in the RDD?
			val rdd2 = sc.textFile("/loudacre/accounts/*")
			rdd2.toDebugString
			//		there is as number of files as number of partitions
			//	6)Use foreachPartition to print the first record of each partition.
			rdd2.foreachPartition(p => println(p.next))

			/**
				* SETTING UP THE JOB
				*/
			//	7)Create an RDD of accounts, keyed by ID and with the string: first_name, last_name for the value
			val rdd = sc.textFile("/loudacre/accounts/*").map(line => line.split(",")).map(arr => (arr(0),arr(4) + "," + arr(3)) )
			//	8)Construct a userReqs RDD with the total number of web hits for each user ID:
			val userReqs = sc.textFile("/loudacre/weblogs/*2.log").map(line => line.split(" ")).map(arr => (arr(2), 1)).reduceByKey((v1, v2) => v1 + v2)
			//	9)Then join the two RDDs by user ID, and construct a new RDD with first name, last name, and total hits:
			val join = rdd.join(userReqs)
			val res = join.map(pair => pair._2)
			//	10)Print the results of res.toDebugString and review the output.
			res.toDebug String

			//(18) MapPartitionsRDD[13] at map at <console>:33 []
			// |   MapPartitionsRDD[11] at join at <console>:31 []
			// |   MapPartitionsRDD[10] at join at <console>:31 []
			// |   CoGroupedRDD[9] at join at <console>:31 []
			// +-(5) MapPartitionsRDD[3] at map at <console>:27 []
			// |  |  MapPartitionsRDD[2] at map at <console>:27 []
			// |  |  /loudacre/accounts/* MapPartitionsRDD[1] at textFile at <console>:27 []
			// |  |  /loudacre/accounts/* HadoopRDD[0] at textFile at <console>:27 []
			// |   ShuffledRDD[8] at reduceByKey at <console>:27 []
			// +-(18) MapPartitionsRDD[7] at map at <console>:27 []
			//    |   MapPartitionsRDD[6] at map at <console>:27 []
			//    |   /loudacre/weblogs/*2.log MapPartitionsRDD[5] at textFile at <console>:27 []
			//    |   /loudacre/weblogs/*2.log HadoopRDD[4] at textFile at <console>:2...
			//a)How many stages are in the job?
			//b)Which stages are dependent on which?
			//c)How many tasks will each stage consist of?


			// To have the opportunity to view the web console of Spark: http://localhost:4040/
			println("Type whatever to the console to exit......")
			scala.io.StdIn.readLine()
		} finally {
			sc.stop()
			println("SparkContext stopped.")
			spark.stop()
			println("SparkSession stopped.")
		}
	}
}

