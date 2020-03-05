/** Question 44
  * Problem Scenario 49 : You have been given below code snippet (do a sum of values by key}, with intermediate output.
  * val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")
  * val data = sc.parallelize(keysWithValuesList)
  * //Create key value pairs
  * val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()
  * val initialCount = 0;
  * val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
  * Now define two functions (addToCounts, sumPartitionCounts) such, which will produce following results.
  * Output 1
  * countByKey.collect
  * res3: Array[(String, Int)] = Array((foo,5), (bar,3))
  * import scala.collection._
  * val initialSet = scala.collection.mutable.HashSet.empty[String]
  * val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
  * Now define two functions (addToSet, mergePartitionSets) such, which will produce following results.
  * Output 2:
  * uniqueByKey.collect
  * res4: Array[(String, scala.collection.mutable.HashSet[String])] = Array((foo,Set(B, A)), (bar,Set(C, D)))
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

import scala.collection._

object question44 {

  val spark = SparkSession
    .builder()
    .appName("question44")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question44")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def addToCounts(z: Int, v: String): Int = z + 1
  def sumPartitionsCounts(v: Int, c:Int): Int = v + c

  def addToSet(z: mutable.HashSet[String],v: String): mutable.HashSet[String] = z + v
  def mergePartitionSets(v: mutable.HashSet[String], c: mutable.HashSet[String]): mutable.HashSet[String] = v ++ c

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")
      val data = sc.parallelize(keysWithValuesList)
      // Create key value pairs
      val kv = data
        .map(d => d.split("="))
        .map(v => (v(0),v(1)))
        .cache()

      val initialCount = 0;

      val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionsCounts)

      countByKey
        .collect
        .foreach(println)

      println("**********************")

      val initialSet = scala.collection.mutable.HashSet.empty[String]

      val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)

      uniqueByKey
        .collect
        .foreach(println)

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
val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")
val data = sc.parallelize(keysWithValuesList)
//Create key value pairs
val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()
val initialCount = 0;
def addToCounts(z: Int, v: String): Int = {z + 1}
def sumPartitionCounts(v: Int, c: Int): Int = {v + c}
val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
countByKey.collect
// res14: Array[(String, Int)] = Array((foo,5), (bar,3))

import scala.collection._
val initialSet = scala.collection.mutable.HashSet.empty[String]
def addToSet(z: mutable.HashSet[String], v: String): mutable.HashSet[String] = {z + v}
def mergePartitionSets(v: mutable.HashSet[String],c: mutable.HashSet[String]): mutable.HashSet[String] = {v ++ c}
val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
uniqueByKey.collect
// res16: Array[(String, scala.collection.mutable.HashSet[String])] = Array((foo,Set(B, A)), (bar,Set(C, D)))
 */