/** Question 19
  * Problem Scenario 50 : You have been given below code snippet (calculating an average score}, with intermediate output.
  * type ScoreCollector = (Int, Double)
  * type PersonScores = (String, (Int, Double))
  * val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0),("Wilma", 95.0), ("Wilma", 98.0))
  * val wilmaAndFredScores = sc.parallelize(initialScores).cache()
  * val scores = wilmaAndFredScores.combineByKey(createScoreCombiner, scoreCombiner, scoreMerger)
  * val averagingFunction = (personScore: PersonScores) => { val (name, (numberScores,totalScore)) = personScore; (name, totalScore / numberScores) }
  * val averageScores = scores.collectAsMap().map(averagingFunction)
  * Expected output: averageScores: scala.collection.Map[String,Double] = Map(Fred -> 91.33333333333333, Wilma -> 95.33333333333333)
  * Define all three required function , which are input for combineByKey method, e.g. (createScoreCombiner, scoreCombiner, scoreMerger). And help us producing required results.
  */
type ScoreCollector = (Int, Double)
type PersonScores = (String, (Int, Double))

val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0),("Wilma", 95.0), ("Wilma", 98.0))

def createScoreCombiner(v: Double): ScoreCollector = {
  (1, v)
}

def scoreCombiner(c: ScoreCollector, v: Double): ScoreCollector = {
  (c._1 + 1, v + c._2)
}

def scoreMerger(c: ScoreCollector, v: ScoreCollector): ScoreCollector = {
  (c._1 + v._1, c._2 + v._2)
}

val wilmaAndFredScores = sc.parallelize(initialScores).cache()

val scores = wilmaAndFredScores.combineByKey(createScoreCombiner, scoreCombiner, scoreMerger)

val averagingFunction = (personScore: PersonScores) => { val (name, (numberScores,totalScore)) = personScore; (name, totalScore / numberScores) }

val averageScores = scores.collectAsMap().map(averagingFunction)

// averageScores: scala.collection.Map[String,Double] = Map(Fred -> 91.33333333333333, Wilma -> 95.33333333333333)