/** Question 19
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

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object question19 {

  val spark = SparkSession
    .builder()
    .appName("question19")
    .master("local")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question19")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  type ScoreCollector = (Int, Double)
  type PersonScores = (String, (Int, Double))

  def createScoreCombiner(i:Double): ScoreCollector = {
    new ScoreCollector(1, i)
  }

  def scoreCombiner(c: ScoreCollector, v:Double): ScoreCollector = {
    new ScoreCollector(c._1 + 1, c._2 + v)
  }

  def scoreMerger(c: ScoreCollector, c1: ScoreCollector): ScoreCollector = {
    new ScoreCollector(c._1 + c1._1, c._2 + c1._2)
  }

  def averagingFunction = (personScore: PersonScores) => {
    val (name, (numberScores, totalScore)) = personScore
    (name, totalScore / numberScores)
  }

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0),("Wilma", 95.0), ("Wilma", 98.0))
      val wilmaAndFredScores = sc
        .parallelize(initialScores)
        .cache()

      val scores = wilmaAndFredScores.combineByKey(createScoreCombiner, scoreCombiner, scoreMerger)

      scores
        .collectAsMap()
        .map(averagingFunction)
        .foreach(println)

      // averageScores: scala.collection.Map[String,Double] = Map(Fred -> 91.33333333333333, Wilma -> 95.33333333333333)

      wilmaAndFredScores.unpersist()

      // To have the opportunity to view the web console of Spark: http://localhost:4040/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("SparkContext stopped.")
      spark.stop()
      println("SparkSession stopped.")
    }
  }
}

/*SOLUTION IN THE SPARK REPL
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
*/