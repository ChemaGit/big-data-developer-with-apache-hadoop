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
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1 : Create all three files in hdfs (We will do using Hue). However, you can first create in local filesystem and then upload it to hdfs. 
$ gedit EmployeeName.csv EmployeeSalary.csv &
$ hdfs dfs -put EmployeeName.csv EmployeeSalary.csv /files
$ hdfs dfs -cat /files/EmployeeName.csv
$ hdfs dfs -cat /files/EmployeeSalary.csv

//Step 2 : Load EmployeeName.csv file from hdfs and create PairRDDs 
val name = sc.textFile("spark5/EmployeeName.csv") 
val namePairRDD = name.map(x => (x.split(",")(0),x.split(",")(1))) 

//Step 3 : Load EmployeeSalary.csv file from hdfs and create PairRDDs 
val salary = sc.textFile("spark5/EmployeeSalary.csv") 
val salaryPairRDD = salary.map(x => (x.split(",")(0),x.split(",")(1))) 

//Step 4 : Join all pairRDDS 
val joined = namePairRDD.join(salaryPairRDD)

//Step 5 : Remove key from RDD and Salary as a Key. 
val keyRemoved = joined.values 

//Step 6 : Now swap filtered RDD. 
val swapped = keyRemoved.map(item => item.swap) 

//Step 7 : Now groupBy keys (It will generate key and value array) 
val grpByKey = swapped.groupByKey().collect() 

//Step 8 : Now create RDD for values collection 
val rddByKey = grpByKey.map{case (k,v) => k->sc.makeRDD(v.toSeq)} 

//Step 9 : Save the output as a Text file. 
rddByKey.foreach{case (k,rdd) => rdd.repartition(1).saveAsTextFile("spark5/Employee" + k)}

/***********ANOTHER SOLUTION*****************/
//Step 1: we create the files and send them from file system to HDFS
$ gedit EmployeeName.csv EmployeeSalary.csv &
$ hdfs dfs -put EmployeeName.csv EmployeeSalary.csv /files
$ hdfs dfs -cat /files/EmployeeName.csv
$ hdfs dfs -cat /files/EmployeeSalary.csv

//Step 2: Now write a Spark code in scala which will load these two files from hdfs and join the same, and produce the (name, salary) values.
val name = sc.textFile("/files/EmployeeName.csv").map(l => l.split(",")).map(arr => (arr(0), arr(1)))
val salary = sc.textFile("/files/EmployeeSalary.csv").map(l => l.split(",")).map(arr => (arr(0), arr(1)))
val joined = name.join(salary).groupBy({case( (id,(name, salary)) ) => salary})

//Step 3: And save the data in multiple file group by salary (Means each file will have name of employees with same salary). Make sure file name include salary as well.
joined.collect.foreach({case(salary, iter) => {sc.parallelize(iter.toList).map({case( (id,(name, salary)) ) => name}).repartition(1).saveAsTextFile("/salary3/" + salary)}})