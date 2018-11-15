/**
 * Problem Scenario 47 : You have been given below code snippet, with intermediate output.
 * val z = sc.parallelize(List(1,2,3,4,5,6), 2)
 * // lets first print out the contents of the RDD with partition labels
 * def myfunc(index: Int, iter: lterator[(Int)]): Iterator[String] = {
 * iter.toList.map(x => "[partID: " + index + ", val: " + x + "]").iterator
 * //In each run , output could be different, while solving problem assume belowm output only.
 * z.mapPartitionsWithIndex(myfunc).collect
 * res28: Array[String] = Array([partID:0, val: 1], [partID:0, val: 2], [partID:0, val: 3], [partID:1,val: 4], [partID:1, val: 5], [partID:1, val: 6])
 * Now apply aggregate method on RDD z , with two reduce function , first will select max value in each partition and second will add all the maximum values from all partitions.
 * Initialize the aggregate with value 5. hence expected output will be 16.
 */

val z = sc.parallelize(List(1,2,3,4,5,6), 2)
def myfunc(index: Int, iter: Iterator[(Int)]): Iterator[String] = { iter.toList.map(x => "[partID: " + index + ", val: " + x + "]").iterator }
z.mapPartitionsWithIndex(myfunc).collect()
// res28: Array[String] = Array([partID:0, val: 1], [partID:0, val: 2], [partID:0, val: 3], [partID:1,val: 4], [partID:1, val: 5], [partID:1, val: 6])
// Answer : 
z.aggregate(5)((init, v) => math.max(init, v), (v1, v2) => v1 + v2) //res: 16
// Or :
z.aggregate(5)(math.max(_,_), (_ + _) ) //res: 16