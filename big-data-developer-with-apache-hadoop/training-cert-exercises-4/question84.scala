/** Question 84
  * Problem Scenario 33 : You have given a files as below.
  * spark5/EmployeeName.csv (id,name)
  * spark5/EmployeeSalary.csv (id,salary)
  * Data is given below:
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
  * Now write a Spark code in scala which will load these two files from hdfs and join the same, and produce the (name, salary) values.
  * And save the data in multiple file group by salary (Means each file will have name of employees with same salary). Make sure file name include salary as well.
  *
  * $ gedit /home/cloudera/files/EmployeeName.csv &
  * $ gedit /home/cloudera/files/EmployeeSalary.csv &
  * $ hdfs dfs -put /home/cloudera/files/EmployeeName.csv  /user/cloudera/files
  * $ hdfs dfs -put /home/cloudera/files/EmployeeSalary.csv /user/cloudera/files
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object question84 {

  val spark = SparkSession
    .builder()
    .appName("question84")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "question84")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/files/"

  val output = "hdfs://quickstart.cloudera/user/cloudera/exercises/question_84/salary_"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val emp = sc
        .textFile(s"${path}EmployeeName.csv")
        .map(line => line.split(","))
        .map(arr => (arr(0),arr(1)))

      val salary = sc
        .textFile(s"${path}EmployeeSalary.csv")
        .map(line => line.split(","))
        .map(arr => (arr(0), arr(1)))

      val joined = salary
        .join(emp)
        .map({case( (id,(sal, name)) ) => (sal, name)})
        .groupByKey()

      joined.foreach({
        case(s, n) => sc
          .makeRDD(n.toList)
          .saveAsTextFile(s"$output$s")
      })

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
$ gedit /home/cloudera/files/EmployeeName.csv &
$ gedit /home/cloudera/files/EmployeeSalary.csv &
$ hdfs dfs -put /home/cloudera/files/EmployeeName.csv /user/cloudera/files
$ hdfs dfs -put /home/cloudera/files/EmployeeSalary.csv /user/cloudera/files

val emp = sc.textFile("/user/cloudera/files/EmployeeName.csv").map(line => line.split(",")).map(r => (r(0),r(1)))
val sal = sc.textFile("/user/cloudera/files/EmployeeSalary.csv").map(line => line.split(",")).map(r => (r(0),r(1)))
val joined = emp.join(sal).map({case( (id,(name, salary)) ) => (salary,name)}).groupByKey().collect
val format = joined.map({case( (salary, name) ) => (sc.makeRDD(List("%s ==> %s".format(salary,name.mkString("[",",","]")))),salary) })
format.foreach({case( (rdd, salary) ) => rdd.saveAsTextFile("/user/cloudera/question84/salary_" + salary)})

$ hdfs dfs -ls /user/cloudera/question84/
$ hdfs dfs -cat /user/cloudera/question84/salary_10000/part*
$ hdfs dfs -cat /user/cloudera/question84/salary_45000/part*
$ hdfs dfs -cat /user/cloudera/question84/salary_50000/part*
*/