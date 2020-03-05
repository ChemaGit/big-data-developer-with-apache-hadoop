import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

/** Question 37
  * Problem Scenario 37 : ABCTECH.com has done survey on their Exam Products feedback
  * using a web based form. With the following free text field as input in web ui.
  * Name: String
  * Subscription Date: String
  * Rating : String
  * And servey data has been saved in a file called spark9/feedback.txt
  * Christopher|Jan 11, 2015|5
  * Kapil|11 Jan, 2015|5
  * Thomas|6/17/2014|5
  * John|22-08-2013|5
  * Mithun|2013|5
  * Jitendra||5
  * Write a spark program using regular expression which will filter all the valid dates and save
  * in two separate file (good record and bad record)
  */

object question37 {

    val spark = SparkSession
      .builder()
      .appName("question37")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
      .config("spark.app.id", "question37")  // To silence Metrics warning
      .getOrCreate()

    val sc = spark.sparkContext

    val input = "hdfs://quickstart.cloudera/user/cloudera/files/feedback.txt"
    val output = "hdfs://quickstart.cloudera/user/cloudera/question37/"

    def regFilterDate(date: String): Boolean = {
        val dateR = date + "\n"
        val regex1 = """(\d{1,2})-(\d{1,2})-(\d{4})\n""".r //22-08-2013
        val regex2 = """(\d{1,2})/(\d{1,2})/(\d{4})\n""".r //6/17/2014
        val regex3 = """([a-zA-Z]{3}) (\d{1,2}), (\d{4})\n""".r //Jan 11, 2015
        val regex4 = """(\d{1,2}) ([a-zA-Z]{3}), (\d{4})\n""".r //11 Jan, 2015

        !(regex1.findAllIn(dateR).isEmpty &&
          regex2.findAllIn(dateR).isEmpty &&
          regex3.findAllIn(dateR).isEmpty &&
          regex4.findAllIn(dateR).isEmpty)
    }

    def main(args: Array[String]): Unit = {

        Logger.getRootLogger.setLevel(Level.ERROR)

        try {
            val data = sc
              .textFile(input)
              .map(line => line.split('|'))
              .cache()

            val good = data
              .filter(r => regFilterDate(r(1)))

            good
              .collect
              .foreach(x => println(x.mkString("|")))

            val bad = data
              .filter(r => !regFilterDate(r(1)))

            bad
              .collect
              .foreach(x => println(x.mkString("|")))

            good
              .map(r => r.mkString("|"))
              .saveAsTextFile(s"${output}good")

            bad
              .map(r => r.mkString("|"))
              .saveAsTextFile(s"${output}bad")

            //      check the results
            //      $ hdfs dfs -ls /user/cloudera/question37/good
            //      $ hdfs dfs -cat /user/cloudera/question37/good/part*
            //      $ hdfs dfs -ls /user/cloudera/question37/bad
            //      $ hdfs dfs -cat /user/cloudera/question37/bad/part*


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
def regFilterDate(date: String): Boolean = {
    val dateR = date + "\n"
    val regex1 = """(\d{1,2})-(\d{1,2})-(\d{4})\n""".r //22-08-2013
    val regex2 = """(\d{1,2})/(\d{1,2})/(\d{4})\n""".r //6/17/2014
    val regex3 = """([a-zA-Z]{3}) (\d{1,2}), (\d{4})\n""".r //Jan 11, 2015
    val regex4 = """(\d{1,2}) ([a-zA-Z]{3}), (\d{4})\n""".r //11 Jan, 2015

    !(regex1.findAllIn(dateR).isEmpty && regex2.findAllIn(dateR).isEmpty && regex3.findAllIn(dateR).isEmpty && regex4.findAllIn(dateR).isEmpty)
}

val data = sc.textFile("/user/cloudera/files/feedback.txt").map(line => line.split('|'))
val good = data.filter(r => regFilterDate(r(1)))
good.collect.foreach(x => println(x.mkString("|")))

val bad = data.filter(r => !regFilterDate(r(1)))
bad.collect.foreach(x => println(x.mkString("|")))

good.map(r => r.mkString("|")).repartition(1).saveAsTextFile("/user/cloudera/question37/good")
bad.map(r => r.mkString("|")).repartition(1).saveAsTextFile("/user/cloudera/question37/bad")

$ hdfs dfs -ls /user/cloudera/question37/good
$ hdfs dfs -cat /user/cloudera/question37/good/part*
$ hdfs dfs -ls /user/cloudera/question37/bad
$ hdfs dfs -cat /user/cloudera/question37/bad/part*
*/