/** Question 75
  * Problem Scenario 54 : You have been given below code snippet.
  * val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"))
  * val b = a.map(x => (x.length, x))
  * operation1
  * Write a correct code snippet for operation1 which will produce desired output, shown below.
  * Array[(Int, String)] = Array((4,lion), (7,panther), (3,dogcat), (5,tigereagle))
  */
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"))
val b = a.map(x => (x.length, x))
b.reduceByKey( (v, c) => v + c).collect
// res0: Array[(Int, String)] = Array((4,lion), (5,tigereagle), (3,dogcat), (7,panther))

// another solution could be
b.foldByKey("")( (v,c) => v + c).collect
//res1: Array[(Int, String)] = Array((4,lion), (5,tigereagle), (3,dogcat), (7,panther))