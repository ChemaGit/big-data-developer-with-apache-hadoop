````scala
val mydata = sc.textFile("purplecow.txt")
mydata.foreach(x => println(x))
mydata.count()
mydata.take(2).foreach(x => println(x))
mydata.collect().foreach(println)
val upper = mydata.map(line => line.toUpperCase)
upper.collect().foreach(x => println(x))
val fil = upper.filter(line => line.startsWith("I"))
fil.collect().foreach(println)
mydata.saveAsTextFile("/user/training/res.txt")
fil.saveAsTextFile("/user/training/resFil.txt") 
upper.toDebugString
upper.saveAsTextFile("/user/training/resUpper.txt") 
upper.saveAsTextFile("/user/training/upper")
````

