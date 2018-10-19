/**
 * Problem Scenario 63 : You have been given below code snippet.
 * val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
 * val b = a.map(x => (x.length, x))
 * operation1
 * Write a correct code snippet for operation1 which will produce desired output, shown below.
 * Array[(Int, String)] = Array((4,lion), (3,dogcat), (7,panther), (5,tigereagle))
 */
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
val b = a.map(x => (x.length, x)).reduceByKey({case(v1, v2) => v1 + v2}).collect