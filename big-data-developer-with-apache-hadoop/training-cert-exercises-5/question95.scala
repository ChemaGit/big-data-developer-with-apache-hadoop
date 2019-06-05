/** Question 95
 * Problem Scenario 59 : You have been given below code snippet.
 * val x = sc.parallelize(1 to 20)
 * val y = sc.parallelize(10 to 30) 
 * operation1
 * z.collect
 * Write a correct code snippet for operation1 which will produce desired output, shown below.
 * Array[Int] = Array(16,12, 20,13,17,14,18,10,19,15,11)
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : val z = x.intersection(y) 
//intersection : Returns the elements in the two RDDs which are the same.

val x = sc.parallelize(1 to 20)
val y = sc.parallelize(10 to 30) 
val z = x.intersection(y)
z.collect()