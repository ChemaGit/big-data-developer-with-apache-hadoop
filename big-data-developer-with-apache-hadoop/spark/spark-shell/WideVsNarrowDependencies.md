````scala
/**
 * LINEAGES
 * Computations on RDDs are represented as a lineage graph; a Directed
 * Acyclic Graph(DAG) representing the computations done on the RDD.
 */
 val wordfile = "/loudacre/files/catsat.txt"
 val rdd = sc.textFile(wordfile)
 val filtered = rdd.map(line => line.split(" ")).filter(r => r(0) == "abc").persist()
 val count = filtered.count()
 val reduced = filtered.reduce(_ + _)
 //input file ==> rdd(map, filter) ==> filtered ==> count
 //                                    filtered ==> reduced

/**
 * RDDs are represented as 
 * Partitions: Atomic pieces of the dataset. One or many per compute node.
 * Dependencies: Models relationship between this RDD and its partitions
 * with the RDD(s) it was derived from.
 * A function: for computing the dataset based on its parent RDDs.
 * Metadata: about its partitioning scheme and data placement.
 */
 
 /**
  * Rule of thumb: a shuffle can occur when the resulting RDD depends on 
  * other elements from the same RDD or another RDD.
  * RDD dependencies encode when data must move across the network.
  * Transformations cause shuffles. Two kinds of dependencies:
  *  1. Narrow Dependencies: Each partition of the parent RDD is used by at most one partition of the child RDD.
  *     Fast! No shuffle necessary. Optimizations like pipelining possible.
  *  2. Wide Dependencies: Each partition of the parent RDD may be depended on by multiple child partitions.
  *     Slow! Requires all or some data to be shuffled over the network.
  *     
  * Transformations with narrow dependencies: map, mapValues, flatMap, filter, mapPartitions, mapPartitionsWithIndex
  * Transformations with wide dependencies: cogroup, groupWith, join, leftOuterJoin, rightOuterJoin, groupByKey, reduceByKey,
  * combineByKey, distinct, intersection, repartition, coalesce    
  */
 
  /**
   * How can I find out?
   * 
   * dependencies method on RDDs.
   */
   val wordsRDD = sc.parallelize(largeList)
   val pairs = wordsRDD.map(c => (c, 1)).groupByKey().dependencies
   // pairs: Seq[org.apache.spark.Dependency[_]]
   // List(org.apache.spark.ShuffleDependency@4294a23d)

  /**
   * How can I find out?
   * 
   * toDebugString method on RDDs.
   */
   val wordsRDD = sc.parallelize(largeList)
   val pairs = wordsRDD.map(c => (c, 1)).groupByKey().toDebugString
   //pairs: String =
   //(8) ShuffledRDD[219] at groupByKey at <console>:38 []
   //+-(8) MapPartitionsRDD[218] at map at <console>:37 []
   //   | ParallelCollectionRDD[217] at parallelize at <console>:36 []

  /**
   * Lineages and Fault Tolerance
   * Lineages graphs are the key to fault tolerance in Spark.
   * Recomputing missing partitions fast for narrow dependencies. 
   * But slow for wide dependencies!
   */
````   
   