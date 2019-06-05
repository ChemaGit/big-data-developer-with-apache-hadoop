#Apache Spark on a cluster
#RDD on a cluster
  *RDDs on a cluster
    -Resilient distribuited datasets
    	Data is partitioned across worker nodes
    -Partitioning is done automatically by Spark
      	Optionally, you can control how many partitions are created
#File Partitioning: Single Files
  *Partitions from single files
    -Partitions based on size
    -You can optionally specify a minimum number of partitions
    	textFile(file, minPartitions)
    -Default is two when running on a cluster
    -Default is one when running locally with a single thread
    -More partitions = more paralellization	
#File Partitioning: Multiples files
  *sc.textFile("mydir/*")
    -Each file becomes(at least) one partition
    -File-based operations can be done per-partition, for example parsing XML
  *sc.wholeTextFiles("mydir")
    -For many small files
    -Creates a key-value PairRDD
    	key = file name
    	value = file contents
#Operating on Partitions
  *Most RDD operations work on each element of an RDD
  *A few work on each partition
  	-foreachPartition calls a function for each partition
  		def printFirstLine(iter: Iterator[Any]) = {
  			println(iter.next)
  		}
  		
  		val myrdd = .....
  		myrdd.foreachPartition(printFirstLine)
  	-mapPartitions creates a new RDD by executing a function on each partition in the current RDD
  	-mapPartitionsWithIndex works the same as mapPartitions but includes index of the partition
  *Functions for partition operations take iterators
  
#HDFS and Data Locality
  *By default, Spark partitions file-based RDDs by block.
  *Each block loads into a single partition.  	 
  *An action triggers execution: tasks on executors load data from blocks into partitions.
  *Data is distribuited across executors until an action returns a value to the driver.
    -sc.textFile("hdfs://...mydata").collect()
#Parallel Operations on Partitions
  *RDD operations are executed in parallel on each partition
    -When possible, tasks execute on the worker nodes where the data is in stored
  *Some operations preserve partitioning
    -Such as map, flatMap, or filter
  *Some operations repartition   
    -Such as reduceByKey, sortByKey, join, or groupByKey  
    
#Stages
  *Operations that can run on the same partition are executed in stages
  *Tasks within a stage are pipelined together
  *Developers should be aware of stages to improve performance
    -STAGE 0
      val avglens = sc.textFile(myfile).
                     flatMap(line => line.split(" ")).
                     map(word => (word(0), word.length)).
    -STAGE 1
                     groupByKey().
                     map(pair => (pair._1, pair._2.sum / pair._2.size().toDouble))
      avglens.saveAsTextFile("avglen-output")   
#Summary of Spark Terminology
  *Job --> a set of tasks executed as a result of an action
  *Stage --> a set of tasks in a job that can be executed in parallel
  *Task --> an individual unit of work sent to one executor
  *Application --> the set of jobs managed by a single driver
#How Spark Calculates Stages
  *Spark contructs a DAG (directed Acyclic Graph) of RDD dependencies
  *Narrow dependencies
    -Each partition in the child RDD depends on just one partition of the parent RDD
    -No shuffle required between executors
    -Can be collapsed into a single stage
    -Examples: map, filter, and union
  *Wide(or shuffle) dependencies
    -Child partitions depend on multiple partitions in the parent RDD
    -Defines a new stage
    -Examples: reduceByKey, join, and groupByKey
#Controlling the level of Parallelism
  *Wide operations(such as reduceByKey) partition resulting RDDs
    -More partitions = more parallel tasks
    -Cluster will be under-utilized if there are too few partitions
  *You can control how many partitions
    -Optional numPartitions parameter in function call
      words.reduceBuKey( (v1, v2) => v1 + v2, 15)
    -Configure the spark.default.parallelism property
      spark.default.parallelism	10
    -The default is the number of partitions of the parent if you do not specify either
    
#Viewing Stages in the	Spark Application UI 
  *You can view jobs and stages in the Spark Application UI
#Viewing the Stages Using toDebugString
  > avglens.toDebugString()
    (2) MappedRDD[5] at map at ...              -STAGE 1
     |  ShuffledRDD[4] at groupByKey at ...
     +-(4) MappedRDD[3] at map at ...           -STAGE 2
        | FlatMappedRDD[2] at flatMap at ...
        | myfile MappedRDD[1] at textFile at ...
        | myfile HadoopRDD[0] at textFile at ...
      
  Indents indicate stages(shuffle boundaries)    
                                     
                                	      
  	      		