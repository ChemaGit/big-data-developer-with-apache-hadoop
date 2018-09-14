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