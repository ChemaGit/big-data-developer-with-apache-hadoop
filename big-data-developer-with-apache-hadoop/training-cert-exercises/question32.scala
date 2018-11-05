/**
 * Problem Scenario 39 : You have been given two files
 * spark16/file1.txt
 * 1,9,5
 * 2,7,4
 * 3,8,3
 * spark16/file2.txt
 * 1,g,h
 * 2,i,j
 * 3,k,l
 * Load these two tiles as Spark RDD and join them to produce the below results
 * (1,((9,5),(g,h))) (2, ((7,4), (i,j))) (3, ((8,3), (k,l)))
 * And write code snippet which will sum the second columns of above joined results (5+4+3).
 */

//Explanation: Solution : 
//Step 1 : Create files in hdfs using Hue. 
//Step 2 : Create pairRDD for both the files. 
val one = sc.textFile("spark16/file1.txt").map{ _.split(",",-1) match { case Array(a, b, c) => (a, ( b, c)) } } 
val two = sc.textFile("spark16/file2.txt").map{ _.split(",",-1) match { case Array(a, b, c) => (a, (b, c)) } } 
//Step 3 : Join both the RDD. 
val joined = one.join(two) 
//Step 4 : Sum second column values. 
val sum = joined.map { case (_, ((_, num2), (_, _))) => num2.toInt }.reduce(_ + _)