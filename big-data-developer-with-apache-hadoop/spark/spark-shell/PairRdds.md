````scala
/**
 * Transformations and Actions on Pair RDD's
 * GroupByKey
 */     
case class Event(team: String, player: String, budget: Int)
val listEvent:List[Event] = List(Event("Barcelona", "Messi", 40), Event("Barcelona", "Suarez", 30), Event("Barcelona", "Dembele", 25), Event("Madrid", "Bale", 30),Event("Madrid", "Benzema", 20), Event("Madrid", "Isco", 10))
val myRdd = sc.parallelize(listEvent).map(event => (event.team, event.budget))
val groupedRdd = myRdd.groupByKey()
groupedRdd.collect().foreach(println)

//ReduceByKey
//ReduceByKey is more efficient than groupByKey
case class Event(team: String, player: String, budget: Int)
val listEvent:List[Event] = List(Event("Barcelona", "Messi", 40), Event("Barcelona", "Suarez", 30), Event("Barcelona", "Dembele", 25), Event("Madrid", "Bale", 30),Event("Madrid", "Benzema", 20), Event("Madrid", "Isco", 10))
val myRdd = sc.parallelize(listEvent).map(event => (event.team, event.budget))
val budgetRdd = myRdd.reduceByKey(_ + _)
budgetRdd.collect().foreach(println)

//MapValues and CountByKey
case class Event(team: String, player: String, budget: Int)
val listEvent:List[Event] = List(Event("Barcelona", "Messi", 40), Event("Barcelona", "Suarez", 30), Event("Barcelona", "Dembele", 25), Event("Madrid", "Bale", 30),Event("Madrid", "Benzema", 20), Event("Madrid", "Isco", 10))
val myRdd = sc.parallelize(listEvent).map(event => (event.team, event.budget))
val intermediate = myRdd.mapValues(b => (b, 1)).reduceByKey((v,v1) => (v._1 + v1._1, v._2 + v1._2))
intermediate.collect().foreach(println)
val avgBudgets = intermediate.mapValues{case(budget, numberOfEvents) => budget / numberOfEvents}
avgBudgets.collect().foreach(println)

//keys
case class Event(team: String, player: String, budget: Int)
val listEvent:List[Event] = List(Event("Barcelona", "Messi", 40), Event("Barcelona", "Suarez", 30), Event("Barcelona", "Dembele", 25), Event("Madrid", "Bale", 30),Event("Madrid", "Benzema", 20), Event("Madrid", "Isco", 10))
val myRdd = sc.parallelize(listEvent).map(event => (event.team, event.budget))
val numsTeams = myRdd.keys.distinct().count()

/**
 * For a list of all available specialized Pair RDD operations, see the Spark
 * API page for PairRDDFunctions (ScalaDoc):
 * http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions
 */

/**
 * PairRDD example 
 */
// read a file with format userid[tab]firstname lastname
val userfile = "/loudacre/files/userlist.tsv"
val users = sc.textFile(userfile).map(line => line.split('\t')) .map(fields => (fields(0),fields(1)))
users.collect()

// order file format: orderid:skuid,skuid,skuid...
// map to RDD of skuids keyed by orderid
val orderfile = "/loudacre/files/orderskus.txt"
val orderskus = sc.textFile(orderfile).map(line => line.split('\t')).map(fields => (fields(0),fields(1))).flatMapValues(skus => skus.split(':'))
for (pair <- orderskus.take(5)) {
    println(pair._1)
    pair._2.foreach(print)
}

// Read zip code, latitude, longitude from a file, map to (zip,(lat,lon))
val zipcoordfile = "/loudacre/files/latlon.tsv"
val zipcoords = sc.textFile(zipcoordfile).map(line => line.split('\t')).map(fields => (fields(0),(fields(1),fields(2))))
for ((zip,coords) <- zipcoords.take(5)) println( "Zip code: " + zip + " at " + coords)

// count words in a file
var wordfile = "/loudacre/files/catsat.txt"
var counts = sc.textFile(wordfile).
    flatMap(_.split(' ')).
    map((_,1)).
    reduceByKey(_+_)

counts.take(10).foreach(println)

// Same thing, shortcut syntax
var counts2 = sc.textFile(wordfile).
    flatMap(line => line.split(' ')).
    map(word => (word,1)).
    reduceByKey((v1,v2) => v1+v2)

for (pair <- counts2.take(5)) println (pair)

// Joining by key example
val movieGross = List(("Casablanca","$3.7M"),("Star Wars","$775M"),("Annie Hall","$38M"),("Argo","$232M"))
val movieGrossRDD = sc.parallelize(movieGross)
val movieYear = List(("Casablanca",1942),("Star Wars",1977),("Annie Hall",1977),("Argo",2012))
val movieYearRDD = sc.parallelize(movieYear)
movieGrossRDD.join(movieYearRDD).foreach(println)

/**
 * Use Pair RDDs to Join Two Datasets
 */  
 //Count the number of request from each user.
 val dir = "/loudacre/weblogs/*2.log"
 val rdd = sc.textFile(dir).map({case(line) => line.split(" ")})
 val rddLog = rdd.map({case(line) => (line(2), 1)})
 val rddGroup = rddLog.reduceByKey({case(v, v1) => v + v1}).sortByKey()
 //determine how many users visited the site for each frequency. That is, how many users visited once, twice, three times, and so on.
 val rddInv = rddGroup.map({case(k, v) => (v, k)})
 val frequency = rddInv.countByKey()
 //Create a tuple where the user ID is the key, and the value is the list of all the IP addresses that user has connected from
 val rddIp = rdd.map({case(arr) => (arr(2), arr(0))})
 //Join	the	accounts data with the weblog data to produce a dataset keyed by user	
 //ID which contains the user account information and the number of website hits for that user.	
 val dirAccounts = "/loudacre/accounts/*"
 val rddAcc = sc.textFile(dirAccounts).map({case(line) => line.split(",")}).map({case(arr) => (arr(0), arr)}).sortByKey()
 val rddJoin = rddAcc.join(rddGroup).persist()
 //Display the user ID, hit count, and first name (4th value) and last name	(5th value)	for the	first 5	elements.
 rddJoin.take(5).foreach({case(k, (v, v1)) => println(k + " " + v1 + " " + v(3) + " " + v(4))})
 //Save in a file the user ID, hit count, and first name (4th value) and last name	(5th value)
 rddJoin.map({case(k, (v, v1)) => k + " " + v1 + " " + v(3) + " " + v(4)}).saveAsTextFile("/loudacre/result/")
 
 //Use keyBy	to create an RDD of	account	data with the postal code (9th field in the	 CSV file) as the key.
val rddPostal = sc.textFile(dirAccounts).map({case(line) => line.split(",")}).keyBy({case(arr) => arr(8)}) 	
//Create a pair RDD with postal code as the key and a list of names(Last Name, First Name) in that postal code as the value
val rddCode = rddPostal.map({case(k, arr) => (k, (arr(4), arr(3)))})
val rddCodeGroup = rddCode.groupByKey().sortByKey()
rddCodeGroup.sortByKey().take(10).foreach({case(k, it) => {println("---" + k); for( (l, f) <- it ) println(l + ", " + f)}})
rddCodeGroup.sortByKey().take(5).foreach({case(k, it) => {println("---" + k); it.foreach(println)}})
rddCodeGroup.sortByKey().take(5).foreach({case(k, it) => {println("---" + k); it.foreach({case(l, f) => println(l + ", " + f)})}})
````

