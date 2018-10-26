/**
 * Problem Scenario 46 : You have been given below list in scala (name,sex,cost) for each work done.
 * List( ("Deeapak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female",2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000))
 * Now write a Spark program to load this list as an RDD and do the sum of cost for combination of name and sex (as key)
 */
//Solution
val rdd = sc.parallelize(List( ("Deeapak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female",2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000)))
val rddKey = rdd.map({case(n,s,p) => ((n,s),p)}).reduceByKey((v1, v2) => v1 + v2)
rddKey.repartition(1).saveAsTextFile("spark12/result.txt")

//Another Solution : 
//Step 1 : Create an RDD out of this list 
val rdd = sc.parallelize(List( ("Deeapak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female", 2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000))) 
//Step 2 : Convert this RDD in pair RDD 
val byKey = rdd.map({case (name,sex,cost) => (name,sex)->cost}) 
//Step 3 : Now group by Key 
val byKeyGrouped = byKey.groupByKey 
//Step 4 : Nowsum the cost for each group 
val result = byKeyGrouped.map{case ((id1,id2),values) => (id1,id2,values.sum)} 
//Step 5 : Save the results 
result.repartition(1).saveAsTextFile("spark12/result.txt")