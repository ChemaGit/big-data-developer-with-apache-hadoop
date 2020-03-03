/** Question 60
  * Problem Scenario 30 : You have been given three csv files in hdfs as below.
  * EmployeeName.csv with the field (id, name)
  * EmployeeManager.csv (id, managerName)
  * EmployeeSalary.csv (id, Salary)
  * Using Spark and its API you have to generate a joined output as below and save as a text file (Separated by comma) for final distribution and output must be sorted by id.
  * /user/cloudera/question60/output
  * output => id,name,salary,managerName
  * EmployeeManager.csv
  * E01,Vishnu
  * E02,Satyam
  * E03,Shiv
  * E04,Sundar
  * E05,John
  * E06,Pallavi
  * E07,Tanvir
  * E08,Shekhar
  * E09,Vinod
  * E10,Jitendra
  * EmployeeName.csv
  * E01,Lokesh
  * E02,Bhupesh
  * E03,Amit
  * E04,Ratan
  * E05,Dinesh
  * E06,Pavan
  * E07,Tejas
  * E08,Sheela
  * E09,Kumar
  * E10,Venkat
  * EmployeeSalary.csv
  * E01,50000
  * E02,50000
  * E03,45000
  * E04,45000
  * E05,50000
  * E06,45000
  * E07,50000
  * E08,10000
  * E09,10000
  * E10,10000
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object question60 {
  /**
    * create the files in the local system
    * $ gedit /home/cloudera/files/EmployeeManager.csv
    * $ gedit /home/cloudera/files/EmployeeName.csv
    * $ gedit /home/cloudera/files/EmployeeSalary.csv
    *
    * put the files from the local system to HDFS
    * $ hdfs dfs -put /home/cloudera/files/EmployeeManager.csv /user/cloudera/files
    * $ hdfs dfs -put /home/cloudera/files/EmployeeName.csv /user/cloudera/files
    * $ hdfs dfs -put /home/cloudera/files/EmployeeSalary.csv /user/cloudera/files
    *
    * output => id,name,salary,managerName
    */

  val spark = SparkSession
    .builder()
    .appName("question60")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question60")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val pathIn = "hdfs://quickstart.cloudera/user/cloudera/files/"
  val pathOut = "hdfs://quickstart.cloudera/user/cloudera/exercise_10/output"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      import spark.implicits._

      val empManager = sc
        .textFile(s"${pathIn}EmployeeManager.csv")
        .map(line => line.split(","))
        .map(r => (r(0), r(1)))
        .toDF("idMan", "managerName")
        .cache()

      val empName = sc
        .textFile(s"${pathIn}EmployeeName.csv")
        .map(line => line.split(","))
        .map(r => (r(0), r(1)))
        .toDF("idEmp", "name")
        .cache()

      val empSalary = sc
        .textFile(s"${pathIn}EmployeeSalary.csv")
        .map(line => line.split(","))
        .map(r => (r(0), r(1)))
        .toDF("idSal", "salary")
        .cache()

      empManager.createOrReplaceTempView("manager")
      empName.createOrReplaceTempView("employee")
      empSalary.createOrReplaceTempView("salary")

      sqlContext
        .sql(
          """SELECT idMan, name, salary, managerName
            |FROM employee JOIN manager ON(idEmp = idMan) JOIN salary ON(idEmp = idSal)
            |ORDER BY idMan""".stripMargin)
        .show()

      val result = sqlContext
        .sql(
          """SELECT idMan, name, salary, managerName
            |FROM employee JOIN manager ON(idEmp = idMan) JOIN salary ON(idEmp = idSal)
            |ORDER BY idMan""".stripMargin)

      result
        .rdd
        .map(r => r.mkString(","))
        .saveAsTextFile(pathOut)

      empManager.unpersist()
      empName.unpersist()
      empSalary.unpersist()

      /**
        * Check the results
        * $ hdfs dfs -ls /user/cloudera/exercise_10/output
        * $ hdfs dfs -cat /user/cloudera/exercise_10/output/part-00000
        */

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
$ gedit /home/cloudera/files/EmployeeManager.csv
$ gedit /home/cloudera/files/EmployeeName.csv
$ gedit /home/cloudera/files/EmployeeSalary.csv
$ hdfs dfs -put /home/cloudera/files/EmployeeManager.csv /user/cloudera/files
$ hdfs dfs -put /home/cloudera/files/EmployeeName.csv /user/cloudera/files
$ hdfs dfs -put /home/cloudera/files/EmployeeSalary.csv /user/cloudera/files

val empManager = sc.textFile("/user/cloudera/files/EmployeeManager.csv").map(line => line.split(",")).map(r => (r(0),r(1)))toDF("idMan","managerName")
val empName = sc.textFile("/user/cloudera/files/EmployeeName.csv").map(line => line.split(",")).map(r => (r(0),r(1)))toDF("idEmp","name")
val empSalary = sc.textFile("/user/cloudera/files/EmployeeSalary.csv").map(line => line.split(",")).map(r => (r(0),r(1).toInt))toDF("idSal","salary")

empManager.registerTempTable("manager")
empName.registerTempTable("employee")
empSalary.registerTempTable("salary")

val result = sqlContext.sql("""SELECT idMan,name,salary,managerName FROM employee JOIN manager ON(idEmp = idMan) JOIN salary ON(idEmp = idSal) ORDER BY idMan""")

result.rdd.map(r => r.mkString(",")).saveAsTextFile("/user/cloudera/question60/output")

$ hdfs dfs -ls /user/cloudera/question60/output
$ hdfs dfs -cat /user/cloudera/question60/output/part-00000
*/


