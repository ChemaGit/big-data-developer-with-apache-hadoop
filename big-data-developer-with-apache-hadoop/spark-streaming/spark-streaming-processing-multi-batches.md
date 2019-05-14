# APACHE SPARK STREAMING: PROCESSING MULTIPLE BATCHES

* In this chapter you will learn

	- How to return data from a specific time period in a DStream
	- How to perform analysis using sliding window operations on a DStream
	- How to maintain state values across all time periods in a DStream

* MULTI-BATCH OPERATIONS

* Multi-Batch DStream Operations

	- DStreams consist of a series of "batches" of data
		- Each batch is an RDD
	- Basic DStream operations analyze each batch individually
	- Advanced operations allow you to analyze each batch individually
	- Advanced operations allow you to analyze data collected across batches
		- Slice: allows you to operate on a collection of batches
		- State: allows you to perform cumulative operations
		- Windows: allows you to aggregate data across a sliding time period

* TIME SLICING

* Time Slicing
	
	- DStream.slice(fromTime, toTime)
		- Returns a collection of batch RDDs based on data from the stream
	- StreamingContext.remember(duration)
		- By default, input data is automatically cleared when no RDD's lineage depends on in
		- slice will return no data for time periods for data has already been cleared
		- Use remember to keep data around longer

* STATE OPERATIONS

* State DStreams

	- Use updateStateByKey function to create a state DStream
	
		// Set checkpoint directory to enable checkpointing.
		
		// Required to prevent infinite lineages.
		
		
		ssc.checkpoint("checkpoints") 

		// Compute a state DStream based on the previous states,
		
		// udated with the values from the current batch of request counts.
		
		val totalUserreqs = userreqs.updateStateByKey(updateCount)
		
		totalUserreqs.print()

		def updateCount = (newCounts: Seq[Int], state: Option[Int]) => {
		
			val newCount = newCounts.foldLeft(0)(_ + _)
			
			val previousCount = state.getOrElse(0)
			
			Some(newCount + previousCount)
		}
		

* SLIDING WINDOW OPERATIONS

* Sliding Window Operations

	- Regular DStream operations execute for each RDD based on SSC duration
	- Window operations span RDDs over a given duration
		- For example reduceByKeyAndWindow, countByWindow

	- By default, window operations will execute with an interval the same as the SSC duration
		- For two-second batch duration, window will slide every two seconds

	- You can specify a different slide duration (must be a multiple of the SSC duration)

	- Count and Sort User Request by Window


		val ssc = new StreamingContext(new SparkConf(), Seconds(2))
		
		val logs = ssc.socketTextStream(hostname, port)
		
		// Every 30 seconds, count requests by user over the last five minutes
		val reqcountsByWindow = logs.map(line => line.split(' ')(2), 1).reduceByKeyAndWindow( (v1: Int, v2: Int) => v1 + v2, Minutes(5), Seconds(30))

		//Sort and print the top users for every RDD (every 30 seconds)
		val topreqsByWindow = reqcountsByWindow.map(pair => pair.swap).transform(rdd => rdd.sortByKey(false))
		
		topreqsByWindow.map(pair => pair.swap).print()

		ssc.start()
		
		ssc.awaitTermination()
		

* ESSENTIAL POINTS

	- You can get a "slice" of data from a stream based on absolute start and end times
		- For example, all data received between midnight October 1, 2016 and midnight October 2, 2016
	- You can update state based on prior state
		- For example, total requests by user
	- You can perform operations on "windows" of data
		- For example, number of logins in the last hour		

