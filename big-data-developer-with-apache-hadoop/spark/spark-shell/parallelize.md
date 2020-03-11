````scala
val myData = List("Alice","Carlos","Frank","Barbara")
val myRdd = sc.parallelize(myData)         
myRdd.collect().foreach(x => println(x))
````

