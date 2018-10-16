/**
 * Problem Scenario 58 : You have been given below code snippet.
 * val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2) val b = a.keyBy(_.length) operation1
 * Write a correct code snippet for operation1 which will produce desired output, shown below.
 * Array[(Int, Seq[String])] = Array((4,ArrayBuffer(lion)), (6,ArrayBuffer(spider)),(3,ArrayBuffer(dog, cat)), (5,ArrayBuffer(tiger, eagle)))
 */

// $ spark-shell -i pruebas.scala
// > :load pruebas.scala
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"))
val b = a.keyBy(_.length)
val operation1 = b.groupByKey().collect()