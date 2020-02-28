/** Question 35
  * Problem Scenario 67 : You have been given below code snippet.
  * lines = sc.parallelize(['its fun to have fun,','but you have to know how.'])
  * r1 = lines.map(lambda x: x.replace(',','').replace('.','').lower())
  * r2 = r1.flatMap(lambda x: x.split(' '))
  * r3 = r2.map(lambda x: (x, 1))
  * operation1
  * r5 = r4.map(lambda x:(x[1],x[0]))
  * r6 = r5.sortByKey(ascending=False)
  * r6.take(20)
  * Write a correct code snippet for operationl which will produce desired output, shown below.
  * [(2, 'fun'), (2, 'to'), (2, 'have'), (1, 'its'), (1, 'know'), (1, 'how'), (1, 'you'), (1, 'but')]
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object question35 {

  val spark = SparkSession
    .builder()
    .appName("question35")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question35")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val lines = sc.parallelize(List("its fun to have fun,","but you have to know how."))

      val r1 = lines
        .map(x => x.replace(",","")
          .replace(".","")
          .toLowerCase())

      val r2 = r1.flatMap(x =>  x.split(" "))
      val r3 = r2.map(x =>  (x, 1))
      val r4 = r3.reduceByKey( (v, v1) => v + v1)
      val r5 = r4.map(x => x.swap)
      val r6 = r5.sortByKey(ascending=false)

      r6.collect.foreach(println)

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
// Python => r4 = r3.reduceByKey(lambda v, v1: v + v1)

lines = sc.parallelize(['its fun to have fun,','but you have to know how.'])
r1 = lines.map(lambda x: x.replace(',','').replace('.','').lower())
r2 = r1.flatMap(lambda x: x.split(' '))
r3 = r2.map(lambda x: (x, 1))
r4 = r3.reduceByKey(lambda v, v1: v + v1)
r5 = r4.map(lambda x:(x[1],x[0]))
r6 = r5.sortByKey(ascending=False)
r6.collect()
// [(2, 'fun'), (2, 'to'), (2, 'have'), (1, 'its'), (1, 'know'), (1, 'how'), (1, 'you'), (1, 'but')]


// Scala => val r4 = r3.reduceByKey( (v,c) => v + c)
val lines = sc.parallelize(List("its fun to have fun,","but you have to know how."))
val r1 = lines.map(x => x.replace(",","").replace(".","").toLowerCase)
val r2 = r1.flatMap(x => x.split(" "))
val r3 = r2.map(x => (x, 1))
val r4 = r3.reduceByKey( (v,c) => v + c)
val r5 = r4.map(x => x.swap)
val r6 = r5.sortByKey(false)
r6.collect
// res1: Array[(Int, String)] = Array((2,have), (2,to), (2,fun), (1,its), (1,how), (1,but), (1,you), (1,know))
*/