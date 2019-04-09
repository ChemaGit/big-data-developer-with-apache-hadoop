/**
 * Problem Scenario 52 : You have been given below code snippet.
 * val b = sc.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1))
 * Operation_xyz
 * Write a correct code snippet for Operation_xyz which will produce below output.
 * scalaxollection.Map[lnt,Long] = Map(5 -> 1, 8 -> 1, 3 -> 1, 6 -> 1, 1 -> 6, 2 -> 3, 4 -> 2, 7 ->1)
 */
  val b = sc.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1))
  val rdd = b.countByValue()
  //Alternative bad solution, Whyyyyyyy?
//I don't know why
  val rdd1 = b.map(v => (v, 1L)).countByKey()