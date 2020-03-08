# APACHE SPARK STREAMING: INTRODUCTION TO DSTREAMS
````text
* APACHE SPARK STREAMING OVERVIEW
	- What is Spark Streaming?
		- An extension of core Spark
		- Provides real-time processing of stream data
	
	- Why Spark Streaming?
		- Many big data applications need to process large data streams in real time, such as
			- Continuous ETL
			- Website monitoring
			- Fraud detection
			- Ad monetization
			- Social media analysis
			- Financial market trends

	- Spark Streaming Features
		- Second-scale latencies
		- Scalability and efficient fault tolerance
		- "Once and only once" processing
		- Integrates batch and real-time processing
		- Easy to develop
			- Uses Spark's high-level API

	- Spark Streaming Overview
		- Divide up data stream into batches of n seconds
			- Called a DStream(Discretized Stream)
		- Process each batch in Spark as an RDD
		- Return results of RDD operations in batches

* Example: Streaming Request Count
````

````scala
	object StreamingRequestCount {
		
		def main(args: Array[String]) {
			val sc = new SparkContext()
			val ssc = new StreamingContext(sc, Seconds(2))
			val mystream = ssc.socketTextStream(hostname, port)
			val userreqs = mystream.map(line => line.split(' ')(2), 1).reduceByKey( (x, y) => x + y)
			userreqs.print()

			ssc.start()
			ssc.awaitTermination()
		}
	}
````

````text	
* Example: Configuring StreamingContext
````

````scala
	val sc = new SparkContext()
	val ssc = new SparkStreamingContext(sc, Seconds(2))
````	

````text
	- A StreamingContext is the main entry point for Spark Streaming apps
	- Equivalent to SparkContext in core Spark
	- Configured with the same parameters as a SparkContext, plus batch duration-instance of Milliseconds, Seconds, or Minutes
	- Named ssc by convention

* Streaming Example: Creating a DStream
````

````scala
	val mystream = ssc.socketTextStream(hostname, port)
````

````text
	- Get a DStream("Discretized Stream") from a streaming data source, for example, text from a socket

* Streaming Example: DStream Transformations
````
````scala
	val userreqs = mystream.map(line => line.split(' ')(2), 1).reduceByKey( (x, y) => x + y)
````	

````text
	- DStream operations applied to each batch RDD in the stream
	- Similar to RDD operations-filter, map, reduce, joinByKey, and so on.

* Streaming Example: DStream Result Output
````

````scala
	userreqs.print()
````	

````text
	- Print out the first 10 elements of each RDD

* Streaming Example: Starting the Streams
````

````scala
	ssc.start()
	ssc.awaitTermination()
````

````text
	- start: Starts the execution of all DStreams
	- awaitTermination: waits for all background threads to complete before ending the main thread

* DStreams
	- A DStream is a sequence of RDDs representing a data stream
		Live Data ---> data data data | data data data | data data data | .....
				t0            t1               t2               t3
				RDD@t1              RDD@t2          RDD@t3
		DStream         data		    data            data
				data                data            data
				data                data            data

* Streaming Example Output
	- Starts 2 seconds after ssc.start(time interval t1)
		-------------------------------------------
		Time: 1401219545000 ms
		-------------------------------------------
		(23713,2)
		(53,2)
		(24433,2)
		(127,2)
		(93,2)
		...
		
	- t2: 2 seconds later...
		-------------------------------------------
		Time: 1401219545000 ms
		-------------------------------------------
		(23713,2)
		(53,2)
		(24433,2)
		(127,2)
		(93,2)
		...
		-------------------------------------------
		Time: 1401219547000 ms
		-------------------------------------------
		(42400,2)
		(24996,2)
		(97464,2)
		(161,2)
		(6011,2)
		...
	- t3: 2 seconds later.... Continues until termination
		-------------------------------------------
		Time: 1401219545000 ms
		-------------------------------------------
		(23713,2)
		(53,2)
		(24433,2)
		(127,2)
		(93,2)
		...
		-------------------------------------------
		Time: 1401219547000 ms
		-------------------------------------------
		(42400,2)
		(24996,2)
		(97464,2)
		(161,2)
		(6011,2)
		...
		-------------------------------------------
		Time: 1401219549000 ms
		-------------------------------------------
		(44390,2)
		(48712,2)
		(165,2)
		(465,2)
		(120,2)
		...

* DStream Data Sources
	- DStreams are defined for a given input stream (suc as Unix socket)
		- Created by the Streaming context		
			ssc.socketTextStream(hostname, port)
		- Similar to how RDDs are created by the Spark context
		- Out-of-the-box data sources
			- Network
				- Sockets
				- Services such as Flume, Akka Actors, Kafka, ZeroMQ, or Twitter
			- Files
				- Monitors an HDFS directory for new content

* DStream Operations
	- DStreams operations are applied to every RDD in the stream
		- Executed once per duration
	- Two types of DStream operations
		- Transformations
			- Create a new DStream from an existing one
		- Output operations
			- Write data(for example, to a file system, database, or console)
			- Similar to RDD actions

* DStream Transformations
	- Many RDD transformations are also available on DStreams
		- Regular transformations such as map, flatMap, filter
		- Pair transformations such as reduceByKey, groupByKey, join
	- What if you want to do something else?
		- transform(function)
			- Creates a new DStream by executing function on RDDs in the current DStream

				val distinctDS = myDS.transform(rdd => rdd.distinct())
				
* DStream Output Operations
	- Console output
		- print() prints out the first 10 elements of each RDD, optionally pass an integer to print another number of elements
	- File output
		- saveAsTextFiles saves data as text
		- saveAsObjectFiles saves as serialized object files(SequenceFiles)
	- Executing other functions
		- foreachRDD(functions) performs a function on each RDD in the DStream
		- Function input parameters
			- The RDD on which to perform the function
			- The time stamp of the RDD (optional)

* Saving DStream Results as Files

	userreqs.saveAsTextFiles(".../outdir/reqcounts")

	* reqcounts-timestamp1/part-00000...
	* reqcounts-timestamp2/part-00000...
	* reqcounts-timestamp3/part-00000...

* Scala Example: Find Top Users
````

````scala
	// Transfom each RDD: swap userID/cout, sort by count		
	val userreqs = mystream.map(line => line.split(' ')(2), 1).reduceByKey( (x, y) => x + y)
	userreqs.saveAsTextFiles(path)	
	val sortedreqs = userreqs.map(pair => pair.swap).transform(rdd => rdd.sortByKey)

	// Print out the top 5 users as "User: userID(cout)"	
	sortedreqs.foreachRDD( (rdd,time) => {
		println("Top users @ " + time)		
		rdd.take(5).foreach(pair => printf("User: %s (%s)\n", pair._2, pair._1"))
	})
````

````text
* Example: Find Top Users-Output
	- t1(2 seconds after ssc.start)
	Top users @ 1401219545000 ms
	User: 16261 (8)
	User: 22232 (7)
	User: 66652 (4)
	User: 21205 (2)
	User: 24358 (2)

	- t2(2 seconds later)
	Top users @ 1401219547000 ms
	User: 53667 (4)
	User: 35600 (4)
	User: 62 (2)
	User: 165 (2)
	User: 40 (2)
	
	- t3(2 seconds later)
	Top users @ 1401219547000 ms
	User: 31 (12)
	User: 6734 (10)
	User: 14986 (10)
	User: 72760 (2)
	User: 65335 (2)

	- Continues until termination....

* DEVELOPING STREAMING APPLICATIONS

* Building and Running Spark Streaming Applications
	- Building Spark Streaming applications
		- Link with the main Spark Streaming library(included with Spark)
		- Link with additional Spark Streaming libraries if necessary, for example, Kafka, Flume, Twitter

	- Running Spark Streaming applications
		- Use at least two threads if running locally
		- Adding operations after the Streaming context has been started is unsupported
		- Stopping and restarting the Streaming context is unsupported

* Using Spark Streaming with Spark Shell
	- Spark Streaming is designed for batch applications, not interactive use
	- The Spark shell can be used for limited testing
		- Not intended for production use!
		- Be sure to run the shell on a cluster with at least 2 cores, or locally with at least 2 threads.
			$ spark-shell --master yarn
			$ spark-shell --master 'local[2]'				
````
