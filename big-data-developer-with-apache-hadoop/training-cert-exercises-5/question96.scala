/** Question 96
 * Problem Scenario 62 : You have been given below code snippet.
 * val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
 * val b = a.map(x => (x.length, x))
 * operation1
 * Write a correct code snippet for operation1 which will produce desired output, shown below.
 * Array[(Int, String)] = Array((3,xdogx), (5,xtigerx), (4,xlionx), (3,xcatx), (7,xpantherx),(5,xeaglex))
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : b.mapValues("x" + _ + "x").collect 
//mapValues [Pair] : Takes the values of a RDD that consists of two-component tuples, and applies the provided function to transform each value. 
//T1ien,.it.forms new two-componend tuples using the key and the transformed value and stores them in a new RDD.

val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
val b = a.map(x => (x.length, x))
b.mapValues(v => "x" + v + "x").collect