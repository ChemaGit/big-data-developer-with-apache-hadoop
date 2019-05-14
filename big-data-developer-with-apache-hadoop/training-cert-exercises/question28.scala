/** Question 28
  * Problem Scenario 43 : You have been given following code snippet.
  * val grouped = sc.parallelize( Seq( ( (1,"two"), List((3,4), (5,6)) ) ) )
  * val flattened = grouped.flatMap {A => groupValues.map { value => B } }
  * You need to generate following output.
  * Hence replace A and B
  * Array((1,two,3,4),(1,two,5,6))
  */
val grouped = sc.parallelize( Seq( ( (1,"two"), List((3,4), (5,6)) ) ) )
val flattened = grouped.flatMap{case(v: (Int, String), groupValues:List[(Int,Int)]) => groupValues.map{value => (v._1,v._2,value._1,value._2)}}
flattened.collect

// res2: Array[(Int, String, Int, Int)] = Array((1,two,3,4), (1,two,5,6))