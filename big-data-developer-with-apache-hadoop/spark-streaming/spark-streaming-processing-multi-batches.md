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