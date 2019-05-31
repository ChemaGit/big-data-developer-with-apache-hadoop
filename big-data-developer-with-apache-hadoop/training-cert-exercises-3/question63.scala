/** Question 63
  * Problem Scenario 57 : You have been given below code snippet.
  * val a = sc.parallelize(1 to 9, 3) operation1
  * Write a correct code snippet for operation1 which will produce desired output, shown below.
  * Array[(String, Seq[Int])] = Array((even,ArrayBuffer(2, 4, 6, 8)), (odd,ArrayBuffer(1, 3, 5, 7,9)))
  */
val a = sc.parallelize(1 to 9, 3)
val operation1 = a.groupBy(v => {if(v % 2 == 0) "even" else "odd"})
operation1.collect

// res0: Array[(String, Iterable[Int])] = Array((even,CompactBuffer(2, 4, 6, 8)), (odd,CompactBuffer(1, 3, 5, 7, 9)))