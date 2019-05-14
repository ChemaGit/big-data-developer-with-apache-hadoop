/** Question 21
  * Problem Scenario 46 : You have been given below list in scala (name,sex,cost) for each work done.
  * List( ("Deeapak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female",2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000))
  * Now write a Spark program to load this list as an RDD and do the sum of cost for combination of name and sex (as key)
  */

val data = sc.parallelize(List( ("Deeapak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female",2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000)))

val dataKey = data.map(t => ( (t._1,t._2), t._3))

val result = dataKey.reduceByKey( (v,c) => v + c)

result.collect.foreach(println)