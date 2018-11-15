/** Question 63
 * Problem Scenario 57 : You have been given below code snippet.
 * val a = sc.parallelize(1 to 9, 3) operation1
 * Write a correct code snippet for operationl which will produce desired output, shown below.
 * Array[(String, Seq[Int])] = Array((even,ArrayBuffer(2, 4, 6, 8)), (odd,ArrayBuffer(1, 3, 5, 7,9)))
 */
//Solution
val a = sc.parallelize(1 to 9, 3).groupBy(x => {if (x % 2 == 0) "even" else "odd" }).collect()

/*Another possible solution would be .....*/
val a = sc.parallelize(1 to 9, 3).map({ case(n) => if(n % 2 == 0) ("even", n) else ("odd", n) }).groupByKey.collect()