/**
 * Problem Scenario 43 : You have been given following code snippet.
 * val grouped = sc.parallelize( Seq( ( (1,"two"), List((3,4), (5,6)) ) ) )
 * val flattened = grouped.flatMap {A => groupValues.map { value => B } }
 * You need to generate following output.
 * Hence replace A and B
 * Array((1,two,3,4),(1,two,5,6))
 */
Explanation: Solution : A case (key, groupValues) B (key._1, key._2, value._1, value._2)

val grouped = sc.parallelize( Seq( ( (1,"two"), List((3,4), (5,6)) ) ) )
val flattened = grouped.flatMap {case(key, groupValues) => groupValues.map { value => (key._1,key._2,value._1,value._2) } }.collect()