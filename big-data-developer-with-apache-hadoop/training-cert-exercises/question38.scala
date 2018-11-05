/**
 * Problem Scenario GG : You have been given below code snippet.
 * val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
 * val b = a.keyBy(_.length)
 * val c = sc.parallelize(List("ant", "falcon", "squid"), 2)
 * val d = c.keyBy(_.length)
 * operation1
 * Write a correct code snippet for operation1 which will produce desired output, shown below.
 * Array[(Int, String)] = Array((4,lion))
 */
//Answer : See the explanation for Step by Step Solution and configuration.
//Explanation: Solution : 
b.subtractByKey(d).collect() 
//subtractByKey [Pair] : Very similar to subtract, but instead of supplying a function, the key- component of each pair will be automatically used as criterion for removing items from the first RDD.

//Bad alternative
b.union(d).groupByKey().filter(t => t._2.size < 2).map(t => (t._1,t._2.mkString(""))).collect()