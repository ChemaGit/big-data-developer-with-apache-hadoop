import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/** Question 50
  * Problem Scenario 55 : You have been given below code snippet.
  * val pairRDD1 = sc.parallelize(List( ("cat",2), ("cat", 5), ("book", 4),("cat", 12)))
  * val pairRDD2 = sc.parallelize(List( ("cat",2), ("cup", 5), ("mouse", 4),("cat", 12)))
  * operation1
  * Write a correct code snippet for operation1 which will produce desired output, shown below.
  * Array[(String, (Option[Int], Option[Int]))] = Array((book,(Some(4),None)),(mouse,(None,Some(4))),
  *                                                     (cup,(None,Some(5))), (cat,(Some(2),Some(2)),
  *                                                     (cat,(Some(2),Some(12))), (cat,(Some(5),Some(2))), (cat,(Some(5),Some(12))),
  *                                                     (cat,(Some(12),Some(2))), (cat,(Some(12),Some(12)))]
  */

object question50 {

  val spark = SparkSession
    .builder()
    .appName("question50")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question50")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val pairRDD1 = sc.parallelize(List( ("cat",2), ("cat", 5), ("book", 4),("cat", 12)))
      val pairRDD2 = sc.parallelize(List( ("cat",2), ("cup", 5), ("mouse", 4),("cat", 12)))

      val join = pairRDD1.fullOuterJoin(pairRDD2)

      join.foreach(println)

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
val pairRDD1 = sc.parallelize(List( ("cat",2), ("cat", 5), ("book", 4),("cat", 12)))
val pairRDD2 = sc.parallelize(List( ("cat",2), ("cup", 5), ("mouse", 4),("cat", 12)))

//Explanation: Solution : pairRDD1.fullOuterJoin(pairRDD2).collect
//fullOuterJoin [Pair] Performs the full outer join between two paired RDDs.
//Listing Variants def fullOuterJoin[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (Option[V], Option[W]))]
//def fullOuterJoin[W](other: RDD[(K, W)]}: RDD[(K, (Option[V], Option[W]))]
//def fullOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (Option[V], Option[W]))]

pairRDD1.fullOuterJoin(pairRDD2).collect
// Array[(String, (Option[Int], Option[Int]))] = Array((book,(Some(4),None)), (mouse,(None,Some(4))), (cup,(None,Some(5))),
// (cat,(Some(2),Some(2))), (cat,(Some(2),Some(12))), (cat,(Some(5),Some(2))), (cat,(Some(5),Some(12))), (cat,(Some(12),Some(2))), (cat,(Some(12),Some(12))))
*/