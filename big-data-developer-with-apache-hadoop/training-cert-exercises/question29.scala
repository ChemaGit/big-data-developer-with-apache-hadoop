/**
 * Problem Scenario 61 : You have been given below code snippet.
 * val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
 * val b = a.keyBy(_.length)
 * val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
 * val d = c.keyBy(_.length) 
 * operation1
 * Write a correct code snippet for operationl which will produce desired output, shown below.
 * Array[(Int, (String, Option[String]))] = Array((6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))),(6,(salmon,Some(turkey))), (6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))),
 *                                                (6,(salmon,Some(turkey))), (3,(dog,Some(dog))), (3,(dog,Some(cat))),(3,(dog,Some(dog))), (3,(dog,Some(bee))), (3,(rat,Some(dogg)), (3,(rat,Some(cat))),
 *                                                (3,(rat.Some(gnu))). (3,(rat,Some(bee))), (8,(elephant,None)))
 */
val leftOuterJoin = b.leftOuterJoin.(d).collect()

Explanation: Solution : 
b.leftOuterJoin(d).collect() 
leftOuterJoin [Pair]: Performs an left outer join using two key-value RDDs. Please note that the keys must be generally comparable to make this work 
keyBy : Constructs two-component tuples (key-value pairs) by applying a function on each data item. 
Trie result of the function becomes the key and the original data item becomes the value of the newly created tuples.

val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
val b = a.keyBy(_.length)
val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
val d = c.keyBy(_.length) 
val leftOuterJoin = b.leftOuterJoin(d).collect()