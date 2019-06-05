/** Question 22
  * Problem Scenario 41 : You have been given below code snippet.
  * val au1 = sc.parallelize(List (("a" , Array(1,2)), ("b" , Array(1,2))))
  * val au2 = sc.parallelize(List (("a" , Array(3)), ("b" , Array(2))))
  * Apply the Spark method, which will generate below output.
  * Array[(String, Array[Int])] = Array((a,Array(1, 2)), (b,Array(1, 2)), (a(Array(3)), (b,Array(2)))
  */
val au1 = sc.parallelize(List (("a" , Array(1,2)), ("b" , Array(1,2))))
val au2 = sc.parallelize(List (("a" , Array(3)), ("b" , Array(2))))

au1.union(au2).collect
// res10: Array[(String, Array[Int])] = Array((a,Array(1, 2)), (b,Array(1, 2)), (a,Array(3)), (b,Array(2)))

// or an alternative solution would be

(au1 ++ au2).collect
// res11: Array[(String, Array[Int])] = Array((a,Array(1, 2)), (b,Array(1, 2)), (a,Array(3)), (b,Array(2)))