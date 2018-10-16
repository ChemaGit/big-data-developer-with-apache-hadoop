/**
 * Problem Scenario 53 : You have been given below code snippet.
 * val a = sc.parallelize(1 to 10, 3)
 * operation1 b.collect
 * Output 1
 * Array[lnt] = Array(2, 4, 6, 8,10)
 * operation2
 * Output 2
 * Array[lnt] = Array(1,2, 3)
 * Write a correct code snippet for operation1 and operation2 which will produce desired output, shown above.
 */
// $ spark-shell -i pruebas.scala
// > :load pruebas.scala
val a = sc.parallelize(1 to 10, 3)
val b = a.filter(n => n % 2 == 0)
val c = a.filter(n => n < 4)
b.collect
c.collect