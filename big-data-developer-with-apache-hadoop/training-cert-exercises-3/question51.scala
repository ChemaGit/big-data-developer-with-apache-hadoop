/** Question 51
  * Problem Scenario 72 : You have been given a table named "employee2" with following detail.
  * first_name string
  * last_name string
  * Write a spark script in scala which read this table and print all the rows and individual column values.
  * employee.json
  * {"first_name":"Ankit", "last_name":"Jain"}
  * {"first_name":"Amir", "last_name":"Khan"}
  * {"first_name":"Rajesh", "last_name":"Khanna"}
  * {"first_name":"Priynka", "last_name":"Chopra"}
  * {"first_name":"Kareena", "last_name":"Kapoor"}
  * {"first_name":"Lokesh", "last_name":"Yadav"}
  */
// edit the file
$ gedit employee.json &
  // put the file in hadoop file system
  $ put /home/cloudera/files/employee.json /user/cloudera/files/
// read the file and create a table employee2 with Spark
val employee = sqlContext.read.json("/user/cloudera/files/employee.json")
employee.repartition(1).rdd.map(r => r.mkString(",")).saveAsTextFile("/user/hive/warehouse/hadoopexam.db/employee2")
sqlContext.sql("use hadoopexam")
sqlContext.sql("""create table employee2(first_name string, last_name string) row format delimited fields terminated by "," stored as textfile location "/user/hive/warehouse/hadoopexam.db/employee2" """)

sqlContext.sql("""select * from employee2""").show()

$ hive
  hive> use hadoopexam;
hive> select * from employee2;
hive> describe formatted employee2;
hive> exit;