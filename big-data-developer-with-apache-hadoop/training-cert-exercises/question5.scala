/** Question 5
  * Problem Scenario 53 : You have been given below code snippet.
  * val a = sc.parallelize(1 to 10, 3)
  * operation1 b.collect
  * Output 1
  * Array[Int] = Array(2, 4, 6, 8,10)
  * operation2
  * Output 2
  * Array[Int] = Array(1,2, 3)
  * Write a correct code snippet for operation1 and operation2 which will produce desired output, shown above.
  */
val a = sc.parallelize(1 to 10, 3)
//solution
val b = a.filter(v => v % 2 == 0)
b.collect
// res0: Array[Int] = Array(2, 4, 6, 8, 10)

// solution
val c = a.filter(v => v <= 3)
c.collect
// res1: Array[Int] = Array(1, 2, 3)