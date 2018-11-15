/** Question 60
 * Problem Scenario 30 : You have been given three csv files in hdfs as below.
 * EmployeeName.csv with the field (id, name)
 * EmployeeManager.csv (id, managerName)
 * EmployeeSalary.csv (id, Salary)
 * Using Spark and its API you have to generate a joined output as below and save as a text file (Separated by comma) for final distribution and output must be sorted by id.
 * id,name,salary,managerName
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

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1 : Create all three files in hdfs in directory called spark1 (We will do using Hue). However, you can first create in local filesystem and then copy the files into HDFS

//Step 2 : Load EmployeeManager.csv file from hdfs and create PairRDDs 
val manager = sc.textFile("spark1/EmployeeManager.csv") val managerPairRDD = manager.map(x=> (x.split(",")(0),x.split(",")(1))) 

//Step 3 : Load EmployeeName.csv file from hdfs and create PairRDDs 
val name = sc.textFile("spark1/EmployeeName.csv") val namePairRDD = name.map(x=> (x.split(",")(0),x.split(",")(1))) 

//Step 4 : Load EmployeeSalary.csv file from hdfs and create PairRDDs 
val salary = sc.textFile("spark1/EmployeeSalary.csv") val salaryPairRDD = salary.map(x=> (x.split(",")(0),x.split(",")(1))) 

//Step 4 : Join all pairRDDS 
val joined = namePairRDD.join(salaryPairRDD).join(managerPairRDD) 

//Step 5 : Now sort the joined results, 
val joinedData = joined.sortByKey() 

//Step 6 : Now generate comma separated data. 
val finalData = joinedData.map(v => (v._1, v._2._1._1, v._2._1._2, v._2._2)) 

//Step 7 : Save this output in hdfs as text file. 
finalData.repartition(1).saveAsTextFile("spark1/result.txt")

/**********POSSIBLE SOLUTIONS****************************/

//First: We create the files in the local system and edit all of them
$ gedit EmployeeManager.csv EmployeeName.csv EmployeeSalary.csv &

//Second: We send the files from the local system to HDFS, and check the files
$ hdfs dfs -mkdir /files/employeeCsv
$ hdfs dfs -put EmployeeManager.csv EmployeeName.csv EmployeeSalary.csv /files/employeeCsv
$ hdfs dfs -ls /files/employeeCsv
$ hdfs dfs -cat /files/employeeCsv/EmployeeManager.csv
$ hdfs dfs -cat /files/employeeCsv/EmployeeName.csv
$ hdfs dfs -cat /files/employeeCsv/EmployeeSalary.csv

//Third: Using Spark RDD
$ spark-shell
> val empRdd = sc.textFile("/files/employeeCsv/EmployeeName.csv").map(line => line.split(",")).map(arr => (arr(0), arr(1)))
> val manRdd = sc.textFile("/files/employeeCsv/EmployeeManager.csv").map(line => line.split(",")).map(arr => (arr(0), arr(1)))
> val salRdd = sc.textFile("/files/employeeCsv/EmployeeSalary.csv").map(line => line.split(",")).map(arr => (arr(0), arr(1).toInt))
> val joined = empRdd.join(salRdd).join(manRdd).sortByKey()
//joined: org.apache.spark.rdd.RDD[(String, ((String, Int), String))] = MapPartitionsRDD[17] at join at <console>:33
> val format = joined.map({case( (id,((name, salary),manager)) ) => id + "," + name + "," + salary + "," + manager})
> format.repartition(1).saveAsTextFile("/files/employeeCsv/rddResult")

//Fourth: Using Spark SQL
//#Create database tables in MySQL
//#Scripts Sqoop to export data from HDFS to MySql
sqoop export \
--connect jdbc:mysql://localhost/test \
--username training \
--password training \
--table employee_name \
--export-dir /files/employeeCsv/EmployeeName.csv \
--outdir /home/training/Desktop/outdir \
--bindir /home/training/Desktop/bindir \
--num-mappers 1

sqoop export \
--connect jdbc:mysql://localhost/test \
--username training \
--password training \
--table employee_manager \
--export-dir /files/employeeCsv/EmployeeManager.csv \
--outdir /home/training/Desktop/outdir \
--bindir /home/training/Desktop/bindir \
--num-mappers 1

sqoop export \
--connect jdbc:mysql://localhost/test \
--username training \
--password training \
--table employee_salary \
--export-dir /files/employeeCsv/EmployeeSalary.csv \
--outdir /home/training/Desktop/outdir \
--bindir /home/training/Desktop/bindir \
--num-mappers 1

//#Import data from MySql to Hive
sqoop import \
--connect jdbc:mysql://localhost/test \
--username training \
--password training \
--table employee_name \
--hive-import \
--hive-table test.t_employee_name \
--hive-home /user/hive/warehouse \
--outdir /home/training/Desktop/outdir \
--bindir /home/training/Desktop/bindir \
--num-mappers 1

sqoop import \
--connect jdbc:mysql://localhost/test \
--username training \
--password training \
--table employee_manager \
--hive-import \
--hive-table test.t_employee_manager \
--hive-home /user/hive/warehouse \
--outdir /home/training/Desktop/outdir \
--bindir /home/training/Desktop/bindir \
--num-mappers 1

sqoop import \
--connect jdbc:mysql://localhost/test \
--username training \
--password training \
--table employee_salary \
--hive-import \
--hive-table test.t_employee_salary \
--hive-home /user/hive/warehouse \
--outdir /home/training/Desktop/outdir \
--bindir /home/training/Desktop/bindir \
--num-mappers 1

$ spark-shell
> sqlContext.sql("use test").show()
> sqlContext.sql("show tables").show()
> val result = sqlContext.sql("SELECT N.id, N.name, S.salary, M.manager_name FROM t_employee_name AS N JOIN t_employee_manager AS M ON (N.id = M.id) JOIN t_employee_salary AS S ON (N.id = S.id) ORDER BY N.id")
> result.show()
> result.repartition(1).write.text("/files/employeeCsv/sqlResult")
> val rdd = result.rdd
> val format = rdd.map(row => (row(0).toString, row(1).toString, row(2).toString, row(3).toString) )
> format.map({case(i,n,s,m) => i + "," + n + "," + s + "," + m}).repartition(1).saveAsTextFile("/files/employeeCsv/sqlResult")

//Another way to do this would be....
> case class EmployeeName(id: String, name: String) 
> case class EmployeeManager(id: String, managerName: String)
> case class EmployeeSalary(id: String, salary: Int)
> val empDf = sc.textFile("/files/employeeCsv/EmployeeName.csv").map(line => line.split(",")).map(arr => EmployeeName(arr(0), arr(1))).toDF
> val manDf = sc.textFile("/files/employeeCsv/EmployeeManager.csv").map(line => line.split(",")).map(arr => EmployeeManager(arr(0), arr(1))).toDF
> val salDf = sc.textFile("/files/employeeCsv/EmployeeSalary.csv").map(line => line.split(",")).map(arr => EmployeeSalary(arr(0), arr(1).toInt)).toDF
> empDf.registerTempTable("employee")
> manDf.registerTempTable("manager")
> salDf.registerTempTable("salary")
> val result = sqlContext.sql("SELECT N.id, N.name, S.salary, M.managerName FROM employee AS N JOIN manager AS M ON (N.id = M.id) JOIN salary AS S ON (N.id = S.id) ORDER BY N.id")
> result.show()
> val rdd = result.rdd
> val format = rdd.map(row => (row(0).toString, row(1).toString, row(2).toString, row(3).toString) )
> format.map({case(i,n,s,m) => i + "," + n + "," + s + "," + m}).repartition(1).saveAsTextFile("/files/employeeCsv/sqlResult2")


