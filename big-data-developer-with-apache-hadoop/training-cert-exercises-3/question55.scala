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