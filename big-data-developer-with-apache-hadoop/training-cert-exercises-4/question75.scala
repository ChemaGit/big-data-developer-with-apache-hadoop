/** Question 75
 * Problem Scenario 54 : You have been given below code snippet.
 * val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"))
 * val b = a.map(x => (x.length, x))
 * operation1
 * Write a correct code snippet for operation1 which will produce desired output, shown below.
 * Array[(Int, String)] = Array((4,lion), (7,panther), (3,dogcat), (5,tigereagle))
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
b.foldByKey("")(_ + _).collect 
//foldByKey [Pair] Very similar to fold, but performs the folding separately for each key of the RDD. 
//This function is only available if the RDD consists of two-component tuples Listing Variants 
//def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)] 
//def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDD[(K, V)] 
//def foldByKey(zeroValue: V, partitioner: Partitioner)(func: (V, V) => V): RDD[(K, V)]

val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"))
val b = a.map(x => (x.length, x))
b.foldByKey("")(_ + _).collect
res2: Array[(Int, String)] = Array((4,lion), (5,tigereagle), (3,dogcat), (7,panther))

/*OTHER POSSIBLE SOLUTION WOULD BE...**/
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"))
val b = a.map(x => (x.length, x))
b.reduceByKey(_ + _).collect
res1: Array[(Int, String)] = Array((4,lion), (5,tigereagle), (3,dogcat), (7,panther))