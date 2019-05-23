/** Question 44
  * Problem Scenario 49 : You have been given below code snippet (do a sum of values by key}, with intermediate output.
  * val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")
  * val data = sc.parallelize(keysWithValuesList)
  * //Create key value pairs
  * val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()
  * val initialCount = 0;
  * val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
  * Now define two functions (addToCounts, sumPartitionCounts) such, which will produce following results.
  * Output 1
  * countByKey.collect
  * res3: Array[(String, Int)] = Array((foo,5), (bar,3))
  * import scala.collection._
  * val initialSet = scala.collection.mutable.HashSet.empty[String]
  * val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
  * Now define two functions (addToSet, mergePartitionSets) such, which will produce following results.
  * Output 2:
  * uniqueByKey.collect
  * res4: Array[(String, scala.collection.mutable.HashSet[String])] = Array((foo,Set(B, A)), (bar,Set(C, D)))
  */
val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")
val data = sc.parallelize(keysWithValuesList)
//Create key value pairs
val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()
val initialCount = 0;
def addToCounts(z: Int, v: String): Int = {z + 1}
def sumPartitionCounts(v: Int, c: Int): Int = {v + c}
val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
countByKey.collect
// res14: Array[(String, Int)] = Array((foo,5), (bar,3))

import scala.collection._
val initialSet = scala.collection.mutable.HashSet.empty[String]
def addToSet(z: mutable.HashSet[String], v: String): mutable.HashSet[String] = {z + v}
def mergePartitionSets(v: mutable.HashSet[String],c: mutable.HashSet[String]): mutable.HashSet[String] = {v ++ c}
val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
uniqueByKey.collect
// res16: Array[(String, scala.collection.mutable.HashSet[String])] = Array((foo,Set(B, A)), (bar,Set(C, D)))