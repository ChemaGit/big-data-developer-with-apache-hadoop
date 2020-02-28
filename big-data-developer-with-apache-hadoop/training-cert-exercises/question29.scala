import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

/** Question 29
  * Problem Scenario 61 : You have been given below code snippet.
  * val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
  * val b = a.keyBy(_.length)
  * val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
  * val d = c.keyBy(_.length)
  * operation1
  * Write a correct code snippet for operationl which will produce desired output, shown below.
  * Array[(Int, (String, Option[String]))] = Array((6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))),(6,(salmon,Some(turkey))), (6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))),
  *                                                (6,(salmon,Some(turkey))), (3,(dog,Some(dog))), (3,(dog,Some(cat))),(3,(dog,Some(dog))), (3,(dog,Some(bee))), (3,(rat,Some(dogg)), (3,(rat,Some(cat))),
  *                                                (3,(rat.Some(gnu))). (3,(rat,Some(bee))), (8,(elephant,None)))
  */

object question29 {

  val spark = SparkSession
    .builder()
    .appName("question29")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question29")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)
    try {
      val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
      val b = a.keyBy(_.length)
      val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
      val d = c.keyBy(_.length)

      val result = b.leftOuterJoin(d)
      result.foreach(x => println(x))

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
val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
val b = a.keyBy(_.length)
val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
val d = c.keyBy(_.length)

b.leftOuterJoin(d).collect

// res3: Array[(Int, (String, Option[String]))] = Array((6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))), (6,(salmon,Some(turkey))), (6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))), (6,(salmon,Some(turkey))), (3,(dog,Some(dog))), (3,(dog,Some(cat))), (3,(dog,Some(gnu))), (3,(dog,Some(bee))), (3,(rat,Some(dog))), (3,(rat,Some(cat))), (3,(rat,Some(gnu))), (3,(rat,Some(bee))), (8,(elephant,None)))
*/