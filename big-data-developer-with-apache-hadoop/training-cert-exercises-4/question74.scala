/** Question 74
  * Problem Scenario 45 : You have been given 2 files , with the content as given Below
  * (technology.txt)
  * (salary.txt)
  * (technology.txt)
  * first,last,technology
  * Amit,Jain,java
  * Lokesh,kumar,unix
  * Mithun,kale,spark
  * Rajni,vekat,hadoop
  * Rahul,Yadav,scala
  * (salary.txt)
  * first,last,salary
  * Amit,Jain,100000
  * Lokesh,kumar,95000
  * Mithun,kale,150000
  * Rajni,vekat,154000
  * Rahul,Yadav,120000
  * Write a Spark program, which will join the data based on first and last name and save the joined results in following format, first Last.technology.salary
  *
  * Create the files and put them into HDFS
  * $ gedit /home/cloudera/files/technology.txt
  * $ gedit /home/cloudera/files/salary.txt
  * $ hdfs dfs -put /home/cloudera/files/technology.txt /user/cloudera/files/
  * $ hdfs dfs -put /home/cloudera/files/salary.txt /user/cloudera/files/
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object question74 {

  val spark = SparkSession
    .builder()
    .appName("question74")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question74")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/files/"
  val output = "hdfs://quickstart.cloudera/user/cloudera/exercises/question_74"

  case class Technology(first: String, last: String, tech: String)
  case class Salary(firstN: String, lastN: String, salary: Int)

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)

    import spark.implicits._

    try {
      val techDF = sc
        .textFile(s"${path}technology.txt")
        .map(line => line.split(","))
        .map(r => new Technology(r(0), r(1), r(2)))
        .toDF
        .cache()

      val salaryDF =  sc
        .textFile(s"${path}salary.txt")
        .map(line => line.split(","))
        .map(r => new Salary(r(0), r(1), r(2).toInt))
        .toDF
        .cache()

      techDF.show()
      salaryDF.show()

      techDF.createOrReplaceTempView("technology")
      salaryDF.createOrReplaceTempView("salary")

      val result = sqlContext
        .sql(
          """SELECT first, last, tech, salary
            |FROM technology JOIN salary ON(first = firstN AND last = lastN)
            |ORDER BY salary DESC""".stripMargin)
        .cache()

      result.show()

      result
        .rdd
        .map(r => "%s,%s,%s,%s".format(r(0).toString,r(1).toString,r(2).toString,r(3).toString))
        .saveAsTextFile(output)

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
$ gedit /user/cloudera/files/technology.txt
$ gedit /user/cloudera/files/salary.txt
$ hdfs dfs -put /user/cloudera/files/technology.txt /user/cloudera/files
$ hdfs dfs -put /user/cloudera/files/salary.txt /user/cloudera/files

val tech = sc.textFile("/user/cloudera/files/technology.txt").map(line => line.split(",")).map(r => ( (r(0),r(1)),r(2) ))
val  salary = sc.textFile("/user/cloudera/files/salary.txt").map(line => line.split(",")).map(r => ( (r(0),r(1)),r(2) ))
val joined = tech.join(salary).map({case( ( (f,l),(t,s)) ) => "%s,%s,%s,%s".format(f,l,t,s)})
joined.saveAsTextFile("/user/cloudera/question74/result")

$ hdfs dfs -cat /user/cloudera/question74/result/part*
*/