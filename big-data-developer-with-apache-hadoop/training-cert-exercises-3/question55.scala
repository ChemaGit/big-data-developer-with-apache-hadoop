/** Question 55
  * Problem Scenario 90 : You have been given below two files
  * course.txt
  * id,course
  * 1,Hadoop
  * 2,Spark
  * 3,HBase
  * fee.txt
  * id,fee
  * 2,3900
  * 3,4200
  * 4,2900
  * Accomplish the following activities.
  * 1. Select all the courses and their fees , whether fee is listed or not.
  * 2. Select all the available fees and respective course. If course does not exists still list the fee
  * 3. Select all the courses and their fees , whether fee is listed or not. However, ignore records having fee as null.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object question55 {

  /**
    * create the files and put then in HDFS
    * $ gedit /home/cloudera/files/fee.txt &
    * $ gedit /home/cloudera/files/course.txt &
    * $ hdfs dfs -put /home/cloudera/files/course.txt /user/cloudera/files/
    * $ hdfs dfs -put /home/cloudra/files/fee.txt /user/cloudera/files/
    */

  val spark = SparkSession
    .builder()
    .appName("question55")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question55")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/files/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val courseSchema = StructType(List(StructField("idC", IntegerType, false), StructField("course",StringType, false)))

      val feeSchema = StructType(List(StructField("idF", IntegerType, false), StructField("fee",IntegerType, false)))

      val course = sqlContext
        .read
        .schema(courseSchema)
        .option("header", false)
        .option("sep",",")
        .csv(s"${path}course.txt")
        .cache()

      val fee = sqlContext
        .read
        .schema(feeSchema)
        .option("header", false)
        .option("sep",",")
        .csv(s"${path}fee.txt")
        .cache()

      course.createOrReplaceTempView("course")
      fee.createOrReplaceTempView("fee")

      sqlContext
        .sql(
          """SELECT *
            |FROM course JOIN fee ON(idC = idF)""".stripMargin)
        .show()

      // 1. Select all the courses and their fees , whether fee is listed or not.
      sqlContext.sql(
        """SELECT course, fee
          |FROM course LEFT OUTER JOIN fee ON(idC = idF)""".stripMargin)
        .show()

      // 2. Select all the available fees and respective course. If course does not exists still list the fee
      sqlContext.sql(
        """SELECT course, fee
          |FROM course RIGHT OUTER JOIN fee ON(idC = idF)""".stripMargin)
        .show()

      // 3. Select all the courses and their fees , whether fee is listed or not. However, ignore records having fee as null.
      sqlContext.sql(
        """SELECT course, fee
          |FROM course LEFT OUTER JOIN fee ON(idC = idF)
          |WHERE fee IS NOT NULL""".stripMargin)
        .show()

      sqlContext.sql(
        """SELECT course, fee
          |FROM course JOIN fee ON(idC = idF)""".stripMargin)
        .show()

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
$ gedit /home/cloudera/files/course.txt &
$ gedit /home/cloudera/files/fee.txt &
$ hdfs dfs -put /home/cloudera/files/course.txt /user/cloudera/files
$ hdfs dfs -put /home/cloudera/files/fee.txt /user/cloudera/files

val course = sc.textFile("/user/cloudera/files/course.txt").map(line => line.split(",")).map(r => (r(0).toInt,r(1))).toDF("idC","course")
val fee = sc.textFile("/user/cloudera/files/fee.txt").map(line => line.split(",")).map(r => (r(0).toInt,r(1).toInt)).toDF("idF","fee")

course.registerTempTable("course")
fee.registerTempTable("fee")

sqlContext.sql("""select course, fee from course left outer join fee on(idC = idF)""").show()
sqlContext.sql("""select course, fee from course right outer join fee on(idC = idF)""").show()
sqlContext.sql("""select course, fee from course join fee on(idC = idF)""").show()

/********ANOTHER SOLUTION WITH PAIR RDD***************************/

//First: We create the files in the file local system
$ gedit course.txt fee.txt &

//Second: Now we store the files in HDFS
$ hdfs dfs -mkdir /files/course
$ hdfs dfs -put course.txt fee.txt /files/course
$ hdfs dfs -cat /files/course/course.txt
$ hdfs dfs -cat /files/course/fee.txt

//Step 1: Select all the courses and their fees , whether fee is listed or not.
val rddCourses = sc.textFile("/files/course/course.txt").map(l => l.split(",")).map(arr => (arr(0).toInt, arr(1)))
val rddFee = sc.textFile("/files/course/fee.txt").map(l => l.split(",")).map(arr => (arr(0).toInt, arr(1)))

rddCourses.collect().foreach(println)
rddFee.collect().foreach(println)

val firstJoin = rddCourses.leftOuterJoin(rddFee)
firstJoin.collect().foreach(println)

//Step 2: Select all the available fees and respective course. If course does not exists still list the fee
val secondJoin = rddCourses.rightOuterJoin(rddFee)
secondJoin.collect().foreach(println)

//Step 3: Select all the courses and their fees , whether fee is listed or not. However, ignore records having fee as null.
val thirdJoin = rddCourses.join(rddFee)
thirdJoin.collect().foreach(println)
*/