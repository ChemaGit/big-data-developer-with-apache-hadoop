/**
 * Problem Scenario 50 : You have been given below code snippet (calculating an average score}, with intermediate output.
 * type ScoreCollector = (Int, Double)
 * type PersonScores = (String, (Int, Double))
 * val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0),("Wilma", 95.0), ("Wilma", 98.0))
 * val wilmaAndFredScores = sc.parallelize(initialScores).cache()
 * val scores = wilmaAndFredScores.combineByKey(createScoreCombiner, scoreCombiner, scoreMerger)
 * val averagingFunction = (personScore: PersonScores) => { val (name, (numberScores,totalScore)) = personScore (name, totalScore / numberScores)
 * val averageScores = scores.collectAsMap(}.map(averagingFunction)
 * Expected output: averageScores: scala.collection.Map[String,Double] = Map(Fred -> 91.33333333333333, Wilma -> 95.33333333333333)
 * Define all three required function , which are input for combineByKey method, e.g. (createScoreCombiner, scoreCombiner, scoreMerger). And help us producing required results.
 */
type ScoreCollector = (Int, Double)
type PersonScores = (String, (Int, Double))
val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0),("Wilma", 95.0), ("Wilma", 98.0))
val wilmaAndFredScores = sc.parallelize(initialScores).cache()

val createScoreCombiner = (score: Double) => (1, score) 
val scoreCombiner = (collector: ScoreCollector, score: Double) => { 
  val (numberScores, totalScore) = collector 
  (numberScores + 1, totalScore + score) 
} 
val scoreMerger= (collector: ScoreCollector, collector2: ScoreCollector) => { 
  val (numScores1, totalScore1) = collector
  val (numScores2, totalScore2) = collector2 
  (numScores1 + numScores2, totalScore1 + totalScore2) 
}

val scores = wilmaAndFredScores.combineByKey(createScoreCombiner, scoreCombiner, scoreMerger)
val averagingFunction = (personScore: PersonScores) => { 
  val (name, (numberScores,totalScore)) = personScore
  (name, totalScore / numberScores) 
}
val averageScores = scores.collectAsMap().map(averagingFunction)

/*****SAME SOLUTION********/
type ScoreCollector = (Int, Double)
type PersonScores = (String, (Int, Double))
val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0),("Wilma", 95.0), ("Wilma", 98.0))
val wilmaAndFredScores = sc.parallelize(initialScores).cache()

def createScoreCombiner = (score: Double) => (1, score) 
def scoreCombiner = (collector: ScoreCollector, score: Double) => { 
  val (numberScores, totalScore) = collector 
  (numberScores + 1, totalScore + score) 
} 
def scoreMerger= (collector: ScoreCollector, collector2: ScoreCollector) => { 
  val (numScores1, totalScore1) = collector
  val (numScores2, totalScore2) = collector2 
  (numScores1 + numScores2, totalScore1 + totalScore2) 
}

val scores = wilmaAndFredScores.combineByKey(createScoreCombiner, scoreCombiner, scoreMerger)
def averagingFunction = (personScore: PersonScores) => { 
  val (name, (numberScores,totalScore)) = personScore
  (name, totalScore / numberScores) 
}
val averageScores = scores.collectAsMap().map(averagingFunction)