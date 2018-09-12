sc.textFile("purplecow.txt").map(line => line.toUpperCase()).filter(line => line.startsWith("I")).count()

sc.textFile("purplecow.txt").map(line => line.toUpperCase())
                            .filter(line => line.startsWith("I"))
                            .repartition(1)
                            .saveAsTextFile("/user/training/upperFil")
                            
//Example flatMap and distinct
val rdd = sc.textFile("/user/training/purplecow.txt")
val flat = rdd.flatMap(line => line.split(' ')).distinct()
flat.foreach(x => println(x))  

//Example subtract, zip, union, intersection
val list1 = List("Chicago","Boston","Paris","San Francisco","Tokyo")
val list2 = List("San Francisco","Boston","Amsterdam","Mumbai","McMurdo Station")
val rdd1 = sc.parallelize(list1)
val rdd2 = sc.parallelize(list2)
rdd1.subtract(rdd2).foreach(x => println(x))
rdd1.zip(rdd2).foreach(x => println(x))
rdd1.union(rdd2).foreach(x => println(x))
rdd1.intersection(rdd1).foreach(x => println(x))

/**
* Other RDD operations
* -first => returns the first element of the RDD
* -foreach => applies a function to each element in an RDD
* -top(n) => returns the largest n elemtnes using natural ordering
* Sampling operations
* -sample => creates a new RDD with a sampling of elements
* -takeSample => returns an array of sampled elements
* Double RDD operations
* -Statistical functions, such as mean, sum, variance, and stdev
* -Documented in API for: org.apache.spark.rdd.DoubleRDDFunctions
**/