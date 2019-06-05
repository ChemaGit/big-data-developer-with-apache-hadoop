/** Question 1
  * Problem Scenario 52 : You have been given below code snippet.
  * val b = sc.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1))
  * Operation_xyz
  * Write a correct code snippet for Operation_xyz which will produce below output.
  * scala.collection.Map[Int,Long].Map[Int,Long] = Map(5 -> 1, 8 -> 1, 3 -> 1, 6 -> 1, 1 -> 6, 2 -> 3, 4 -> 2, 7 ->1)
  */
val b = sc.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1))
//Solution
b.countByValue
// res0: scala.collection.Map[Int,Long] = Map(5 -> 1, 1 -> 6, 6 -> 1, 2 -> 3, 7 -> 1, 3 -> 1, 8 -> 1, 4 -> 2)