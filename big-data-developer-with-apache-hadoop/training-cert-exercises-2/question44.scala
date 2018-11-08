/**
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
//Step 1
val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")
val data = sc.parallelize(keysWithValuesList)
val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()
val initialCount = 0;
def addToCounts(init: Int, t: String) = (init + 1)
def sumPartitionCounts(v: Int,v1: Int) = v + v1
val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
countByKey.collect()
//Step 2
import scala.collection._
import scala.collection.mutable.HashSet
val initialSet = scala.collection.mutable.HashSet.empty[String]
def addToSet(initSet: HashSet[String], v: String) = initSet += v
def mergePartitionSets(s:HashSet[String], s1: HashSet[String]) = s ++= s1
val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
uniqueByKey.collect()