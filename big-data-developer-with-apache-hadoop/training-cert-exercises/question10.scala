/** Question 10
  * Problem Scenario 48 : You have been given below Scala code snippet, with intermediate output.
  * We want to take a list of records about people and then we want to sum up their ages and count them.
  * So for this example the type in the RDD will be a List[(String,Int,String)] in the format of (name: NAME, age:AGE, gender:GENDER).
  * The result type will be a tuple that looks like so (Sum of Ages, Count)
  * val people = List( ("Amit", 45,"M"),("Ganga", 43,"F"),("John", 28,"M"),("Lolita", 33,"F"),("Dont Know", 18,"T"))
  * val peopleRdd=sc.parallelize(people) //Create an RDD
  * peopleRdd.aggregate(0,0)(seqOp, combOp) //Output of above line : 167, 5)
  * Now define two operation seqOp and combOp , such that
  * seqOp : Sum the age of all people as well count them, in each partition.
  * combOp : Combine results from all partitions.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object question10 {

  val spark = SparkSession
    .builder()
    .appName("question10")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question10")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val people = List( ("Amit", 45,"M"),("Ganga", 43,"F"),("John", 28,"M"),("Lolita", 33,"F"),("Dont Know", 18,"T"))
      val peopleRdd = sc.parallelize(people) //Create an RDD

      def seqOp(init: (Int, Int), value: (String, Int, String)): (Int, Int) = {
        (init._1 + value._2, init._2 + 1)
      }
      def combOp(v: (Int, Int), c: (Int, Int)): (Int, Int) = {
        (v._1 + c._1, v._2 + c._2)
      }

      val result = peopleRdd.aggregate(0,0)(seqOp, combOp)

      println(result)

      // res0: (Int, Int) = (167,5)

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
val people = List(("Amit",45,"M"),("Ganga",43,"F"),("John",28,"M"),("Lolita",33,"F"),("Dont Know",18,"T"))
val peopleRdd = sc.parallelize(people)

def seqOp(u:(Int, Int),t:(String,Int,String)): (Int,Int) = {
  (u._1 + t._2, u._2 + 1)
}

def combOp(v: (Int, Int),c:(Int,Int)): (Int,Int) = {
  (v._1 + c._1, v._2 + c._2)
}

peopleRdd.aggregate((0,0))(seqOp, combOp)
// res15: (Int, Int) = (167,5)
 */