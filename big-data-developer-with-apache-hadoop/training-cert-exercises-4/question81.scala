/** Question 82
  * Problem Scenario 40 : You have been given sample data as below in a file called /home/cloudera/files/file222.txt
  * 3070811,1963,1096,,"US","CA",,1,
  * 3022811,1963,1096,,"US","CA",,1,56
  * 3033811,1963,1096,,"US","CA",,1,23
  * Below is the code snippet to process this tile.
  * val field= sc.textFile("spark15/file1.txt")
  * val mapper = field.map(x=> A)
  * mapper.map(x => x.map(x=> {B})).collect
  * Please fill in A and B so it can generate below final output
  * Array(Array(3070811,1963,109G, 0, "US", "CA", 0,1, 0),Array(3022811,1963,1096, 0, "US", "CA", 0,1, 56),Array(3033811,1963,1096, 0, "US", "CA", 0,1, 23)
  *
  * Create the files
  * $ gedit /home/cloudera/files/file222.txt &
  * $ hdfs dfs -put /home/cloudera/file222.txt /user/cloudera/files/
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object question81 {

  val spark = SparkSession
    .builder()
    .appName("question81")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question81")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val intput = "hdfs://quickstart.cloudera/user/cloudera/files/file222.txt"

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val field= sc.textFile(intput)
      val mapper = field
        .map(x => x.split(","))
        .cache()

      val result = mapper
        .map(x => x.map(x => {if(x.isEmpty || x == "" || x == " ") 0 else x}))
        .collect

      result.foreach(x => println(x.mkString(",")))

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

/*
$ gedit /home/cloudera/files/file222.txt &
$ hdfs dfs -put /home/cloudera/files/file222.txt /home/cloudera/files

val field= sc.textFile("/user/cloudera/files/file222.txt")
val mapper = field.map(x => x.split(","))
mapper.map(x => x.map(x => {if(x.isEmpty || x == "" || x == " ") 0 else x})).collect

// res2: Array[Array[Any]] = Array(Array(3070811, 1963, 1096, 0, "US", "CA", 0, 1, 0), Array(3022811, 1963, 1096, 0, "US", "CA", 0, 1, 56), Array(3033811, 1963, 1096, 0, "US", "CA", 0, 1, 23))
*/