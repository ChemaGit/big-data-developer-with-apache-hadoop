/**
 * Problem Scenario 60 : You have been given below code snippet.
 * val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
 * val b = a.keyBy(_.length)
 * val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
 * val d = c.keyBy(_.length)
 * operation1
 * Write a correct code snippet for operation1 which will produce desired output, shown below.
 * Array[(Int, (String, String))] = Array((6,(salmon,salmon)), (6,(salmon,rabbit)),(6,(salmon,turkey)), (6,(salmon,salmon)), (6,(salmon,rabbit)),(6,(salmon,turkey)), (3,(dog,dog)), (3,(dog,cat)), (3,(dog,gnu)), (3,(dog,bee)), (3,(rat,dog)),(3,(rat,cat)), (3,(rat,gnu)), (3,(rat,bee)))
 */

//Explanation: solution: 
b.join(d).collect() 
//join [Pair]: Performs an inner join using two key-value RDDs. 
//Please note that the keys must be generally comparable to make this work. 
//keyBy : Constructs two-component tuples (key-value pairs) by applying a function on each data item. 
//The result of the function becomes the data item becomes the key and the original value of the newly created tuples.

val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
val b = a.keyBy(_.length)
val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
val d = c.keyBy(_.length)
b.join(d).collect()