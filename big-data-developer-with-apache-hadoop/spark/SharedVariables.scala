package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Broadcast variables allow the programmer to keep a read-only variable cached
  * on each worker node rather than shipping a copy of it with tasks.
  * They can be used, for example, to give every node a copy of a large input dataset in an efficient manner.
  * After the broadcast variable is created, it should be used instead of the value v
  * in any functions run on the cluster so that v is not shipped to the nodes more than once.
  * In addition, the object v should not be modified after it is broadcast in order to ensure that
  * all nodes get the same value of the broadcast variable (e.g. if the variable is shipped to a new node later).
  *
  * Accumulators are variables that can only be added through an associative operation.
  * It is used to implement counters and sum efficiently in parallel.
  * Spark natively supports numeric type accumulators and standard mutable collections.
  * Programmers can extend these for new types. Only the driver can read the values of the accumulators.
  * The workers can only invoke it to increment the value.
  */
object SharedVariables {

  val spark = SparkSession
    .builder()
    .appName("SharedVariables")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "SharedVariables")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      // Let's create a broadcast variable:
      val broadcastVar = sc.broadcast(Array(1,2,3))
      println(broadcastVar.value)

      // Create the accumulator variable. Type in:
      val accum = sc.accumulator(0)
      sc.parallelize(Array(1,2,3,4)).foreach(x => accum += x)

      println(accum.value)

      // To have the opportunity to view the web console of Spark: http://localhost:4041/
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