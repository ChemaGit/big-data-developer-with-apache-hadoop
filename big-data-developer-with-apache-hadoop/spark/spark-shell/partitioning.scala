/**
 * Customizing a partitioning is only possible on Pair RDDs.
 * 
 * The data within an RDD is split into several partitions.
 * Properties:
 * -Partitions never span multiple machines, i.e., tuples in the same
 * partition are guaranteed to be on the same machine.
 * -Each machine in the cluster contains one or more partitions.
 * -The number of partitions to use is configurable. By default, it equals
 * the total number of cores on all executor nodes.
 *
 * Two kinds of partitioning available in Spark:
 * -Hash partitioning
 * -Range partitioning
 */
  case class CFFPurchase(customerId: Int, destination: String, price: Double)
  val dir = "/loudacre/purchase.txt"
  val purchasesRdd: RDD[CFFPurchase] = sc.textFile(dir)
                                         .map(line => line.split(","))
                                         .map(rec => new CFFPurchase(rec(0).toInt, rec(1), rec(2).toDouble))
                                         
  val purchasesPerCust = purchasesRdd.map(p => (p.customerId, p.price)) //Pair RDD
                                     .groupByKey()
  /**      
   * HASH PARTITIONING                                     
   * groupByKey first computes per tuple (k,v) its partition p: p = k.hashCode() % numPartitions
   * Then, all tuples in the same partition p are sent to the machine hosting p.
   * hash partitioning attemps to spread data evenly across partitions based on the key.
   * 
   * RANGE PARTITIONING
   * Pair RDDs may contain keys that have an ordering defined: Int, Char, String, ...
   * For such RDDs, range partitioning may be more efficient.
   * Using a range partitioner, keys are partitioned according to:
   * 1. an ordering keys
   * 2. a set of sorted ranges of keys
   * Property: tuples with keys in the same range appear on the same machine.
   */
                                     
  /**
   * HASH PARTITIONING: Example 
   * Consider a Pair RDD, with keys [8, 96, 240, 400, 401, 800], and a desired num ber of partitions of 4.
   * Furthermore, suppose that hashCode () is the identity ( n. hashCode () == n).
   * In this case, hash partitioning distributes the keys as follows among the partitions:  
   * partition 0: [8, 96, 240, 400, 800]
   * partition 1: [401]                   
   * partition 2: []
   * partition 3: []
   * The result is a very unbalanced distribution which hurts performance.               
   */
                                     
  /**                                     
   * RANGE PARTITIONING: Example      
   * Using range partitioning the distribution can be improved significantly:
   * Assumptions: (a)keys non-negative, (b)800 is biggest key in the RDD.
   * Set of ranges: [1 , 200], [201 , 400], [ 401 , 600], [601 , 800]
   * 
   * Range partitioning distributes the keys as follows among the partitions:
   * partition 0: [8, 96]
   * partition 1: [240, 400]                   
   * partition 2: [401]
   * partition 3: [800]       
   * The resulting partitioning is much more balanced.                         
   */
                                     
  /**                                     
   * There are two ways to create RDDs with specific partitionings:
   * 1. Call partitionBy on an RDD, providing an explicit Partitioner.
   * 2. Using transformations that return RDDs with specific partitioners.                                      
   */
                                     
  /**
   * partitionBy      
   * 
   * Creating the desired number of partitions requires:
   * 1. Specifying the desired number of partitions.
   * 2. Providing a Pair RDD with ordered keys.
   * Important: the result of partitionBy should be persisted.
   * Otherwise, the partitioning is repeatedly applied (involving shuffling!) each time the partitioned RDD is used.                               
   */
  val pairs = purchasesRdd.map(p => (p.customerId, p.price))
  val tunedPartitioner = new RangePartitioner(8, pairs)
  val partitioned = pairs.partitionBy(tunedPartitioner).persist()
  
  /**
   * Partitioning Data Using Transformations
   * 
   * -Partitioner from parent RDD: Pair RDDs that are result of a transformation on a partitioned
   * Pair RDD typically is configured to use the hash partitioner that was used to construct it.
   * 
   * -Automatically-set partitioners: Some operations on RDDs automatically result in an RDD
   * with a known partitioner - for when it makes sense.
   * For example, by default, when using sortByKey, a RangePartitioner is used.
   * Further, the default partitioner when using groupByKey, is a HashPartitioner.
   * 
   * Operations on Pair RD Ds that hold to ( and propagate) a partitioner:
   * cogrooup, foldByKey, groupWith, combineByKey,
   * join, partitionBy, leftOuterJoin, sort,
   * rightOuterJoin, mapValues(if parent has a partitioner),
   * groupByKey, flatMapValues(if parent has a partitioner),
   * reduceByKey, filter (if parent has a partitioner)
   * 
   * All other operations will produce a result without a partitioner. Why?
   * Consider map transformation.
   * Because it's possible for map to change the key.
   * 
   */
  
  
  /**
   * Optimizing with Partitioners
   * Partitioning can bring substantial performance gains, especially in the face of shuffles.
   * 
   * Using range partitioners we can optimize our earlier use of reduceByKey so
   * that it does not involve any shuffling over the network at all!
   */
   val pairs = purchasesRdd.map(p => (p.customerId, p.price))
   val tunedPartitioner = new RangePartitioner(8, pairs)
   val partitioned = pairs.partitionBy(tunedPartitioner).persist()
   
   val purchasesPerCust = partitioned.map(p => (p._1, (1, p._2)))
   val purchasesPerMonth = purchasesPerCust.reduceByKey((v1,v2) => (v1._1 + v2._1, v1._2 + v2._2)).collect()
   
   /**
    * How do I know a shuffle will occur?
    * 1. The return type of certain transformations, e.g., org.apache.spark.rdd.RDD[(String, Int)]= ShuffledRDD[366]
    * 2. Using function toDebugString to see its execution plan:
    *    partitioned.reduceByKey((v1, v2) => (v1 ._1 + v2._1, v1 ._2 + v2._2)).toDebugString
    *    res9: String=
    *    (8) MapPartitionsRDD[622] at reduceByKey at <console>:49 []
    *    | ShuffledRDD[615] at partitionBy at <console>:48 []
    *    |   CachedPartitions: 8; MemorySize: 1754.8MB; DiskSize: 0.0 B
    *    
    * Operations that might cause a shuffle: cogroup, groupWith, join, leftOuterJoin, rightOuterJoin,
    * groupByKey, reduceByKey, combineByKey, distinct, intersection, repartition, coalesce.   
    */
    */