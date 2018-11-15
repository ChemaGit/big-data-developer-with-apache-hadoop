/**
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

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//Step 1: 
hdfs dfs -mkdir sparksql4 
hdfs dfs -put course.txt sparksql4/ 
hdfs dfs -put fee.txt sparksql4/ 

//Step 2 : Now in spark shell 
// load the data into a new RDD 
val course = sc.textFile("sparksql4/course.txt") 
val fee = sc.textFile("sparksql4/fee.txt") 
// Return the first element in this RDD 
course.first() 
fee.first() 
//define the schema using a case class 
case class Course(id: Int, name: String) 
case class Fee(id: Int, fee: Int) 
// create an RDD of Course objects 
val courseRDD = course.map(_.split(",")).map(c => Course(c(0).toInt,c(1))) 
val feeRDD = fee.map(_.split(",")).map(c => Fee(c(0).toInt,c(1).toInt)) 
courseRDD.first() 
courseRDD.count() 
feeRDD.first() 
feeRDD.count() 
// change RDD of Course objects to a DataFrame 
val courseDF = courseRDD.toDF() 
val feeDF = feeRDD.toDF() 
// register the DataFrame as a temp table 
courseDF.registerTempTable("course") 
feeDF.registerTempTable("fee") 
// Select data from table 
//Step 1: Select all the courses and their fees , whether fee is listed or not.
val results = sqlContext.sql("""SELECT * FROM course""" ) 
results.show() 
val results = sqlContext.sql("""SELECT * FROM fee""") 
results.show() 
val results = sqlContext.sql("""SELECT * FROM course LEFT JOIN fee ON course.id = fee.id""") 
results.show() 

//Step 2: Select all the available fees and respective course. If course does not exists still list the fee
val results = sqlContext.sql("""SELECT * FROM course RIGHT JOIN fee ON course.id = fee.id""") 
results.show() 

//Step 3: Select all the courses and their fees , whether fee is listed or not. However, ignore records having fee as null.
val results = sqlContext.sql("""SELECT * FROM course LEFT JOIN fee ON course.id = fee.id where fee.id IS NOT NULL""")
//Another way 
val results = sqlContext.sql("""SELECT * FROM course JOIN fee ON course.id = fee.id""")
results.show()

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