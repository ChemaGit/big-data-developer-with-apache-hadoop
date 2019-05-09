# APACHE SPARK STREAMING: INTRODUCTION TO DSTREAMS

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
	
* Example: Configuring StreamingContext

	val sc = new SparkContext()
	val ssc = new SparkStreamingContext(sc, Seconds(2))

	- A StreamingContext is the main entry point for Spark Streaming apps
	- Equivalent to SparkContext in core Spark
	- Configured with the same parameters as a SparkContext, plus batch duration-instance of Milliseconds, Seconds, or Minutes
	- Named ssc by convention

* Streaming Example: Creating a DStream

	val mystream = ssc.socketTextStream(hostname, port)

	- Get a DStream("Discretized Stream") from a streaming data source, for example, text from a socket

* Streaming Example: DStream Transformations

	val userreqs = mystream.map(line => line.split(' ')(2), 1).reduceByKey( (x, y) => x + y)

	- DStream operations applied to each batch RDD in the stream
	- Similar to RDD operations-filter, map, reduce, joinByKey, and so on.

* Streaming Example: DStream Result Output

	userreqs.print()

	- Print out the first 10 elements of each RDD

* Streaming Example: Starting the Streams

	ssc.start()
	ssc.awaitTermination()

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
